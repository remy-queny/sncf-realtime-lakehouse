#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON="$PROJECT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "Erreur : environnement .venv absent."
  exit 1
fi

export PYTHONPATH="$PROJECT_ROOT/spark/src"
export PYSPARK_PYTHON="$PYTHON"

echo "=== Vérification Kafka ==="
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic sncf.trip_updates.raw

echo "=== Configuration PostgreSQL ==="
POSTGRES_USER="$(docker compose exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_USER:-postgres}"')"
POSTGRES_DB="$(docker compose exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"')"
POSTGRES_PASSWORD="$(docker compose exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_PASSWORD:-}"')"

export POSTGRES_USER POSTGRES_DB POSTGRES_PASSWORD
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432

if [[ -z "$POSTGRES_PASSWORD" ]]; then
  echo "Erreur : mot de passe PostgreSQL absent du conteneur."
  exit 1
fi

"$PYTHON" -c \
  'import os, psycopg
with psycopg.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    connect_timeout=5,
) as connection:
    connection.execute("SELECT 1")
print("Connexion PostgreSQL OK")'

for job in \
  stream_bronze.py \
  stream_silver.py \
  build_gold_delay_by_line.py \
  build_gold_station_delay_daily.py \
  publish_postgres.py
do
  echo "=== Exécution : $job ==="
  "$PYTHON" "spark/src/jobs/$job"
done

echo "=== Contrôle Silver ==="
"$PYTHON" spark/src/jobs/inspect_silver.py

echo "=== Pipeline terminé ==="