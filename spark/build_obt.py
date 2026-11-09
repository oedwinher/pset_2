"""Construye la One Big Table (OBT) de nowcasting de ventas con PySpark.

Gold (star schema en Snowflake)  ->  Spark (joins + features)  ->  ANALYTICS.OBT_NOWCASTING_VENTAS

Grano de la OBT: una fila por rama CIIU (46) x mes fiscal x corte (temprano_15d | definitivo),
el MISMO grano que GOLD.FCT_VENTAS_MENSUALES. Los joins con las dimensiones son N:1, asi que
no deben cambiar el numero de filas; el script lo valida y aborta antes de escribir si no se cumple.

Ejecucion:
  * En el cluster del docker-compose:  docker compose --profile jobs run --rm spark-submit
  * Local (local[*]):                  python spark/build_obt.py     (requiere Java 17 y .env cargado)
Credenciales: solo variables de entorno SNOWFLAKE_* (ver .env.example).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
CONNECTOR_PACKAGE = "net.snowflake:spark-snowflake_2.12:3.2.3-spark_3.5"
SCHEMA_GOLD = "GOLD"
SCHEMA_SALIDA = "ANALYTICS"
TABLA_SALIDA = "OBT_NOWCASTING_VENTAS"
CLAVE_GRANO = ["codigo_industria", "tiempo_key", "corte_key"]


def cargar_env_local() -> None:
    """Si existe .env en la raiz del repo (ejecucion local), lo carga sin sobreescribir el entorno."""
    env = Path(__file__).resolve().parent.parent / ".env"
    if not env.exists():
        return
    for linea in env.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def opciones_snowflake(schema: str) -> dict[str, str]:
    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
        if not os.environ.get(var):
            sys.exit(f"Falta la variable de entorno {var}")
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfRole": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "PSET2_DB"),
        "sfSchema": schema,
    }


def leer_tabla(spark: SparkSession, tabla: str) -> DataFrame:
    df = (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**opciones_snowflake(SCHEMA_GOLD))
        .option("dbtable", tabla)
        .load()
    )
    return df.toDF(*[c.lower() for c in df.columns])  # Snowflake devuelve MAYUSCULAS


def div_seguro(numerador, denominador):
    """Division que devuelve NULL si el denominador es 0 o NULL (evita infinitos/errores en los ratios)."""
    return F.when(denominador != 0, numerador / denominador)


def validar(condicion: bool, mensaje_ok: str, mensaje_error: str) -> None:
    if not condicion:
        raise SystemExit(f"VALIDACION FALLIDA: {mensaje_error}")
    print(f"[OK] {mensaje_ok}")


def main() -> None:
    cargar_env_local()
    spark = (
        SparkSession.builder.appName("pset2-build-obt-nowcasting")
        .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages", CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "8")  # volumen pequeno (~13k filas)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------ 1. Leer Gold
    fct = leer_tabla(spark, "FCT_VENTAS_MENSUALES")
    dim_ind = leer_tabla(spark, "DIM_INDUSTRIA")
    dim_tmp = leer_tabla(spark, "DIM_TIEMPO")
    dim_cor = leer_tabla(spark, "DIM_CORTE")

    for c in ("ventas_nominales_usd", "ventas_reales_usd", "ventas_por_declarante_usd", "indice_ipc_sim",
              "ratio_temprano_definitivo"):
        fct = fct.withColumn(c, F.col(c).cast("double"))
    fct = fct.withColumn("num_declarantes", F.col("num_declarantes").cast("long")).cache()
    n_fct = fct.count()
    print(f"Filas en FCT_VENTAS_MENSUALES: {n_fct}")

    obt, columnas_salida, n_obt = construir_obt(fct, dim_ind, dim_tmp, dim_cor)
    escribir_obt(spark, obt, columnas_salida, n_obt)


def construir_obt(fct: DataFrame, dim_ind: DataFrame, dim_tmp: DataFrame, dim_cor: DataFrame):
    """Joins + features + validaciones de integridad. Es independiente de Snowflake (testeable)."""
    n_fct = fct.count()

    # ------------------------------------------------------------------ 2. Joins (N:1)
    obt = (
        fct.alias("f")
        .join(dim_ind.alias("i"), "codigo_industria", "left")
        .join(dim_tmp.alias("t"), "tiempo_key", "left")
        .join(dim_cor.alias("c"), "corte_key", "left")
    )
    n_joins = obt.count()
    validar(
        n_joins == n_fct,
        f"Joins con dimensiones conservan el numero de filas ({n_joins} == {n_fct})",
        f"los joins cambiaron el numero de filas: fact={n_fct}, obt={n_joins} (posible producto cartesiano)",
    )
    huerfanos = obt.filter(
        F.col("nombre_industria").isNull() | F.col("fecha_mes").isNull() | F.col("tipo_corte").isNull()
    ).count()
    validar(huerfanos == 0, "Sin claves huerfanas (todas las FK encontraron su dimension)",
            f"{huerfanos} filas sin dimension asociada")

    # ------------------------------------------------------------------ 3. Features
    obt = obt.withColumn("indice_mes", F.col("anio") * 12 + F.col("mes"))  # entero mensual para rezagos

    base = obt.select("codigo_industria", "corte_key", "indice_mes", "ventas_reales_usd", "ventas_nominales_usd")

    def rezago(n: int, sufijo: str) -> DataFrame:
        # Join por indice de mes (no por posicion de fila): robusto a meses faltantes
        return base.select(
            "codigo_industria",
            "corte_key",
            (F.col("indice_mes") + n).alias("indice_mes"),
            F.col("ventas_reales_usd").alias(f"ventas_reales_usd_lag_{sufijo}"),
            F.col("ventas_nominales_usd").alias(f"ventas_nominales_usd_lag_{sufijo}"),
        )

    llave_lag = ["codigo_industria", "corte_key", "indice_mes"]
    obt = obt.join(rezago(1, "1"), llave_lag, "left").join(rezago(12, "12"), llave_lag, "left")

    # Referencia definitiva del mismo mes y rama (denominador del ratio de representatividad)
    definitivo = (
        obt.filter(F.col("es_definitivo"))
        .select(
            "codigo_industria",
            "tiempo_key",
            F.col("ventas_nominales_usd").alias("ventas_nominales_definitivo_mes"),
            F.col("ventas_reales_usd").alias("ventas_reales_definitivo_mes"),
        )
    )
    obt = obt.join(definitivo, ["codigo_industria", "tiempo_key"], "left")

    obt = (
        obt.withColumn(
            "crecimiento_interanual_real_pct",
            F.round((div_seguro(F.col("ventas_reales_usd"), F.col("ventas_reales_usd_lag_12")) - 1) * 100, 4),
        )
        .withColumn(
            "crecimiento_interanual_nominal_pct",
            F.round((div_seguro(F.col("ventas_nominales_usd"), F.col("ventas_nominales_usd_lag_12")) - 1) * 100, 4),
        )
        .withColumn(
            "variacion_mensual_real_pct",
            F.round((div_seguro(F.col("ventas_reales_usd"), F.col("ventas_reales_usd_lag_1")) - 1) * 100, 4),
        )
        # Representatividad del corte: ventas del corte / ventas definitivas del mismo mes (=1 para definitivo)
        .withColumn(
            "ratio_representatividad_corte",
            F.round(div_seguro(F.col("ventas_nominales_usd"), F.col("ventas_nominales_definitivo_mes")), 4),
        )
    )

    # Representatividad historica: promedio de los 12 meses PREVIOS (excluye el mes actual -> sin fuga)
    ventana_12m = (
        Window.partitionBy("codigo_industria", "corte_key").orderBy("indice_mes").rangeBetween(-12, -1)
    )
    obt = obt.withColumn(
        "ratio_representatividad_hist_12m",
        F.round(F.avg("ratio_representatividad_corte").over(ventana_12m), 4),
    ).withColumn(
        "ratio_corte_vs_hist_12m",
        F.round(div_seguro(F.col("ratio_representatividad_corte"), F.col("ratio_representatividad_hist_12m")), 4),
    )

    columnas_salida = [
        # claves y grano
        "venta_key", "codigo_industria", "tiempo_key", "corte_key",
        # dimension industria
        "numero_rama", "nombre_industria", "sector_macro", "sector_economico",
        # dimension tiempo
        "fecha_mes", "anio", "mes", "nombre_mes", "trimestre", "semestre", "anio_trimestre", "indice_mes",
        # dimension corte
        "tipo_corte", "es_definitivo", "dias_aprox_desde_cierre",
        # medidas del hecho
        "ventas_nominales_usd", "ventas_reales_usd", "num_declarantes", "ventas_por_declarante_usd", "indice_ipc_sim",
        # features
        "ventas_reales_usd_lag_1", "ventas_reales_usd_lag_12", "ventas_nominales_usd_lag_1", "ventas_nominales_usd_lag_12",
        "crecimiento_interanual_real_pct", "crecimiento_interanual_nominal_pct", "variacion_mensual_real_pct",
        "ratio_representatividad_corte", "ratio_representatividad_hist_12m", "ratio_corte_vs_hist_12m",
        # referencia/etiqueta (solo disponible a posteriori: NO usar como feature en el corte temprano)
        "ventas_nominales_definitivo_mes", "ventas_reales_definitivo_mes",
        # banderas de calidad
        "flag_ventas_cero_con_declarantes", "flag_temprano_mayor_definitivo",
    ]
    obt = obt.select(*columnas_salida).cache()

    # ------------------------------------------------------------------ 4. Validacion de integridad
    n_obt = obt.count()
    validar(
        n_obt == n_fct,
        f"OBT final conserva el grano del hecho ({n_obt} == {n_fct})",
        f"el numero de filas de la OBT ({n_obt}) difiere del hecho ({n_fct})",
    )
    n_unicos = obt.select(*CLAVE_GRANO).distinct().count()
    validar(n_unicos == n_obt, f"Unicidad del grano: 0 duplicados ({n_unicos} claves distintas)",
            f"hay {n_obt - n_unicos} filas duplicadas en el grano {CLAVE_GRANO}")
    n_pk = obt.select("venta_key").distinct().count()
    validar(n_pk == n_obt, "venta_key unica", f"venta_key con {n_obt - n_pk} duplicados")

    return obt, columnas_salida, n_obt


def escribir_obt(spark: SparkSession, obt: DataFrame, columnas_salida: list, n_obt: int) -> None:
    # ------------------------------------------------------------------ 5. Escribir a Snowflake
    obt_out = obt.toDF(*[c.upper() for c in obt.columns])
    opts_out = opciones_snowflake(SCHEMA_SALIDA)
    try:  # el esquema ANALYTICS puede no existir todavia
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            opts_out, f"CREATE SCHEMA IF NOT EXISTS {opts_out['sfDatabase']}.{SCHEMA_SALIDA}"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Aviso: no se pudo crear el esquema {SCHEMA_SALIDA} ({exc}); se asume existente.")

    (
        obt_out.write.format(SNOWFLAKE_SOURCE)
        .options(**opts_out)
        .option("dbtable", TABLA_SALIDA)
        .mode("overwrite")
        .save()
    )

    leido = (
        spark.read.format(SNOWFLAKE_SOURCE).options(**opts_out).option("dbtable", TABLA_SALIDA).load().count()
    )
    validar(leido == n_obt, f"Reconciliacion post-escritura: {leido} filas en {SCHEMA_SALIDA}.{TABLA_SALIDA}",
            f"se escribieron {n_obt} filas pero Snowflake contiene {leido}")

    print(f"\nOBT lista: {opts_out['sfDatabase']}.{SCHEMA_SALIDA}.{TABLA_SALIDA} ({n_obt} filas, "
          f"{len(columnas_salida)} columnas)")
    spark.stop()


if __name__ == "__main__":
    main()
