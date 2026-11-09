"""Carga cruda (Raw) del Formulario 104 del SRI hacia Snowflake.

Lo ejecuta el flow de Kestra `f104_to_snowflake_raw` dentro de un contenedor
python:3.11-slim. Tambien puede correrse a mano (ver README):

    python load_raw_f104.py --csv ../../data/seed/f104_ventas_46_ramas.csv --modo completa

Decisiones de diseno
--------------------
* RAW es un registro inmutable y *append-only*: todas las columnas de negocio
  se guardan como VARCHAR tal cual vienen de la fuente (sin tipar ni limpiar)
  mas columnas de auditoria (_BATCH_ID, _SOURCE_FILE, _PERIODO_EJECUCION,
  _LOADED_AT). Cada carga queda como una "vintage" distinta; la deduplicacion
  (ultimo registro por clave de negocio) ocurre en dbt Bronze.
* modo=completa  -> carga todo el CSV (primera carga / reconstruccion).
* modo=incremental -> carga los meses fiscales M-1 y M-2 respecto a la fecha de
  referencia (M-1: corte temprano recien disponible; M-2: refresca el corte
  definitivo con rectificatorias). Con un backfill de Kestra, cada ejecucion
  usa su propia fecha de referencia ({{ trigger.date }}), de modo que el
  historico se reconstruye mes a mes.
* Credenciales unicamente por variables de entorno (nunca en el codigo).
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import uuid
from datetime import date, datetime

import snowflake.connector

COLUMNAS_FUENTE = [
    "anio_fiscal",
    "mes_fiscal",
    "codigo_industria_cnt_46",
    "descripcion_industria_cnt_46",
    "ventas_totales",
    "num_declarantes",
    "tipo_corte",
    "mes_fiscal_id",
]
LOTE = 5000


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", required=True, help="Ruta al CSV semilla del F104")
    p.add_argument("--modo", choices=["completa", "incremental"], default="completa")
    p.add_argument("--fecha-ref", default=None, help="Fecha de referencia ISO (trigger.date o execution.startDate)")
    p.add_argument("--reemplazar-raw", default="false", help="true -> TRUNCATE de RAW antes de cargar")
    return p.parse_args()


def mes_anterior(d: date, n: int) -> str:
    """Primer dia del mes (d - n meses) como 'YYYY-MM-01'."""
    total = d.year * 12 + (d.month - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}-01"


def leer_csv(ruta: str) -> list[dict]:
    with open(ruta, newline="", encoding="utf-8-sig") as f:
        lector = csv.DictReader(f)
        faltan = set(COLUMNAS_FUENTE) - set(lector.fieldnames or [])
        if faltan:
            raise SystemExit(f"El CSV no contiene las columnas esperadas: {sorted(faltan)}")
        # Cadena vacia -> NULL (se preserva la ausencia original; no se imputa en RAW)
        return [{c: (fila[c] if fila[c] != "" else None) for c in COLUMNAS_FUENTE} for fila in lector]


def conectar() -> snowflake.connector.SnowflakeConnection:
    requeridas = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    faltan = [v for v in requeridas if not os.environ.get(v)]
    if faltan:
        raise SystemExit(f"Faltan variables de entorno: {faltan}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        session_parameters={"QUERY_TAG": "pset2_kestra_f104_raw"},
    )


def main() -> None:
    args = parse_args()
    db = os.environ.get("SNOWFLAKE_DATABASE", "PSET2_DB")
    ref_txt = (args.fecha_ref or datetime.utcnow().isoformat())[:10]
    ref = date.fromisoformat(ref_txt)

    filas = leer_csv(args.csv)
    total_fuente = len(filas)
    if args.modo == "incremental":
        meses = {mes_anterior(ref, 1), mes_anterior(ref, 2)}
        filas = [f for f in filas if f["mes_fiscal_id"] in meses]
        print(f"[incremental] fecha_ref={ref} -> meses fiscales objetivo: {sorted(meses)}")
    print(f"Filas en fuente: {total_fuente} | filas a cargar: {len(filas)}")
    if not filas:
        print("No hay filas para el periodo solicitado; nada que cargar (la ejecucion es valida).")
        return

    batch_id = str(uuid.uuid4())
    origen = os.path.basename(args.csv)
    con = conectar()
    cur = con.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.RAW")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db}.RAW.RAW_F104_VENTAS (
                ANIO_FISCAL                  VARCHAR,
                MES_FISCAL                   VARCHAR,
                CODIGO_INDUSTRIA_CNT_46      VARCHAR,
                DESCRIPCION_INDUSTRIA_CNT_46 VARCHAR,
                VENTAS_TOTALES               VARCHAR,
                NUM_DECLARANTES              VARCHAR,
                TIPO_CORTE                   VARCHAR,
                MES_FISCAL_ID                VARCHAR,
                _BATCH_ID                    VARCHAR,
                _SOURCE_FILE                 VARCHAR,
                _PERIODO_EJECUCION           VARCHAR,
                _LOADED_AT                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        if args.reemplazar_raw.lower() == "true":
            cur.execute(f"TRUNCATE TABLE {db}.RAW.RAW_F104_VENTAS")
            print("RAW truncada (reemplazar_raw=true)")

        sql = (
            f"INSERT INTO {db}.RAW.RAW_F104_VENTAS "
            "(ANIO_FISCAL, MES_FISCAL, CODIGO_INDUSTRIA_CNT_46, DESCRIPCION_INDUSTRIA_CNT_46, VENTAS_TOTALES, "
            "NUM_DECLARANTES, TIPO_CORTE, MES_FISCAL_ID, _BATCH_ID, _SOURCE_FILE, _PERIODO_EJECUCION) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        for i in range(0, len(filas), LOTE):
            lote = [
                tuple(f[c] for c in COLUMNAS_FUENTE) + (batch_id, origen, ref_txt)
                for f in filas[i : i + LOTE]
            ]
            cur.executemany(sql, lote)
            print(f"  insertadas {min(i + LOTE, len(filas))}/{len(filas)}")

        cur.execute(
            f"SELECT COUNT(*) FROM {db}.RAW.RAW_F104_VENTAS WHERE _BATCH_ID = %s", (batch_id,)
        )
        cargadas = cur.fetchone()[0]
        if cargadas != len(filas):
            raise SystemExit(f"Reconciliacion fallida: esperadas {len(filas)}, cargadas {cargadas}")
        print(f"OK batch={batch_id}: {cargadas} filas cargadas en {db}.RAW.RAW_F104_VENTAS")
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    try:
        main()
    except snowflake.connector.errors.Error as e:
        print(f"Error de Snowflake: {e}", file=sys.stderr)
        sys.exit(1)
