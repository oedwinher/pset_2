#!/usr/bin/env bash
# Sube a Kestra el script de carga, el CSV semilla (como archivos del namespace) y el flow.
# Uso (desde la raiz del repo):  bash kestra/deploy.sh
set -euo pipefail

KESTRA=${KESTRA:-http://localhost:${KESTRA_PORT:-8080}/api/v1/main}
USUARIO=${KESTRA_USER:-admin@kestra.io:Admin1234}
NS=ecuador.bce.coyuntura
FLOW=kestra/flows/f104_to_snowflake_raw.yml
ID=f104_to_snowflake_raw

subir() {  # subir <archivo-local> <nombre-en-namespace>
  curl -sf -u "$USUARIO" -X POST "$KESTRA/namespaces/$NS/files?path=/$2" -F "fileContent=@$1" > /dev/null
  echo "Archivo $2 subido al namespace $NS"
}

subir kestra/scripts/load_raw_f104.py load_raw_f104.py
subir data/seed/f104_ventas_46_ramas.csv f104_ventas_46_ramas.csv

if curl -sf -u "$USUARIO" "$KESTRA/flows/$NS/$ID" > /dev/null; then
  curl -sf -u "$USUARIO" -X PUT -H "Content-Type: application/x-yaml" --data-binary @"$FLOW" "$KESTRA/flows/$NS/$ID" > /dev/null
  echo "Flow $NS.$ID actualizado"
else
  curl -sf -u "$USUARIO" -X POST -H "Content-Type: application/x-yaml" --data-binary @"$FLOW" "$KESTRA/flows" > /dev/null
  echo "Flow $NS.$ID creado"
fi
