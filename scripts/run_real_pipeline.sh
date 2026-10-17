#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage : bash scripts/run_real_pipeline.sh <references_reelles>"
  exit 1
fi

REFERENCE_DIR="$1"

if [[ "$REFERENCE_DIR" != /* ]]; then
  REFERENCE_DIR="$PWD/$REFERENCE_DIR"
fi

if [[ ! -d "$REFERENCE_DIR" ]]; then
  echo "Erreur : dossier de références introuvable : $REFERENCE_DIR"
  exit 1
fi

REFERENCE_DIR="$(cd "$REFERENCE_DIR" && pwd -P)"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "Erreur : environnement .venv absent."
  exit 1
fi

for filename in routes.csv stops.csv manifest.json; do
  if [[ ! -s "$REFERENCE_DIR/$filename" ]]; then
    echo "Erreur : référence absente ou vide : $REFERENCE_DIR/$filename"
    exit 1
  fi
done

export COMPOSE_PROJECT_NAME=sncf-realtime-lakehouse

export PYTHONPATH="$PROJECT_ROOT/spark/src"
export PYSPARK_PYTHON="$PYTHON"

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TRIP_UPDATES_TOPIC=sncf.trip_updates.real.raw

export LAKEHOUSE_ROOT="$PROJECT_ROOT/data/lakehouse_real"
export CHECKPOINT_ROOT="$PROJECT_ROOT/data/checkpoints_real"
export GTFS_REFERENCE_ROOT="$REFERENCE_DIR"

export BRONZE_TRIP_UPDATES_PATH="$LAKEHOUSE_ROOT/bronze/trip_updates"
export BRONZE_TRIP_UPDATES_CHECKPOINT_PATH="$CHECKPOINT_ROOT/bronze_trip_updates"

export POSTGRES_TARGET_SCHEMA=sncf_real
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432

COMPOSE=(docker compose -p "$COMPOSE_PROJECT_NAME")

echo "=== Configuration du traitement réel ==="
printf 'Projet Compose : %s\n' "$COMPOSE_PROJECT_NAME"
printf 'Topic : %s\n' "$KAFKA_TRIP_UPDATES_TOPIC"
printf 'Lakehouse : %s\n' "$LAKEHOUSE_ROOT"
printf 'Checkpoints : %s\n' "$CHECKPOINT_ROOT"
printf 'Références : %s\n' "$GTFS_REFERENCE_ROOT"
printf 'Schéma PostgreSQL : %s\n' "$POSTGRES_TARGET_SCHEMA"

echo "=== Vérification du topic réel ==="

"${COMPOSE[@]}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic "$KAFKA_TRIP_UPDATES_TOPIC"

echo "=== Configuration PostgreSQL ==="

POSTGRES_USER="$("${COMPOSE[@]}" exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_USER:-postgres}"')"

POSTGRES_DB="$("${COMPOSE[@]}" exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"')"

POSTGRES_PASSWORD="$("${COMPOSE[@]}" exec -T postgres sh -c \
  'printf "%s" "${POSTGRES_PASSWORD:-}"')"

export POSTGRES_USER POSTGRES_DB POSTGRES_PASSWORD

if [[ -z "$POSTGRES_PASSWORD" ]]; then
  echo "Erreur : mot de passe PostgreSQL absent du conteneur."
  exit 1
fi

echo "=== Vérification des références et des tables PostgreSQL ==="

"$PYTHON" - <<'PY'
import csv
import json
import os
from pathlib import Path

import psycopg

reference_root = Path(os.environ["GTFS_REFERENCE_ROOT"])

with (reference_root / "manifest.json").open(encoding="utf-8") as file:
    manifest = json.load(file)

if manifest.get("source") != "sncf_gtfs_static":
    raise ValueError("Le manifeste ne décrit pas un référentiel SNCF réel.")

expected_columns = {
    "routes.csv": {"route_id", "line_name"},
    "stops.csv": {"stop_id", "stop_name"},
}

for filename, required in expected_columns.items():
    with (reference_root / filename).open(
        encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.DictReader(file)
        missing = required - set(reader.fieldnames or [])

        if missing:
            raise ValueError(
                f"{filename} : colonnes absentes : {sorted(missing)}"
            )

print("Références réelles : OK")

with psycopg.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    connect_timeout=5,
) as connection:
    connection.execute(
        "SELECT 1 FROM sncf_real.gold_delay_by_line_15min LIMIT 0"
    )
    connection.execute(
        "SELECT 1 FROM sncf_real.gold_station_delay_daily LIMIT 0"
    )

print("Connexion et tables PostgreSQL sncf_real : OK")
PY

for job in \
  stream_bronze.py \
  stream_silver.py \
  inspect_silver.py \
  build_gold_delay_by_line.py \
  build_gold_station_delay_daily.py \
  publish_postgres.py
do
  echo "=== Exécution : $job ==="
  "$PYTHON" "$PROJECT_ROOT/spark/src/jobs/$job"
done

echo "=== Traitement réel terminé ==="
echo "Aucune collecte ni publication Kafka effectuée par ce script."
echo "Les données Gold ont été publiées dans le schéma sncf_real."
echo "Actualiser ensuite la source de données dans Tableau."