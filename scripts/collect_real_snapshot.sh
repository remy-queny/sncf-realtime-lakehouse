#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage : bash scripts/collect_real_snapshot.sh <referentiel_gtfs.zip>"
  exit 1
fi

GTFS_ARCHIVE="$1"

if [[ "$GTFS_ARCHIVE" != /* ]]; then
  GTFS_ARCHIVE="$PWD/$GTFS_ARCHIVE"
fi

if [[ ! -f "$GTFS_ARCHIVE" ]]; then
  echo "Erreur : archive GTFS introuvable : $GTFS_ARCHIVE"
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$PROJECT_ROOT/data/source_checks"

COLLECTION_DIR="$(
  mktemp -d "$PROJECT_ROOT/data/source_checks/collection-XXXXXX"
)"

REALTIME_FILE="$COLLECTION_DIR/trip_updates.pb"
JSONL_FILE="$COLLECTION_DIR/trip_updates.canonical.jsonl"

echo "=== Préparation ==="
printf 'Référentiel : %s\n' "$GTFS_ARCHIVE"
printf 'Dossier de collecte : %s\n' "$COLLECTION_DIR"

cd "$PROJECT_ROOT/producer-java"

echo "=== Compilation et vérification du référentiel ==="

mvn --batch-mode --no-transfer-progress compile exec:java \
  -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsStaticReference \
  -Dexec.args="\"$GTFS_ARCHIVE\""

PYTHON="$PROJECT_ROOT/.venv/bin/python"
REFERENCE_DIR="$PROJECT_ROOT/data/reference/real/$(basename "$COLLECTION_DIR")"

if [[ ! -x "$PYTHON" ]]; then
  echo "Erreur : environnement Python .venv absent."
  exit 1
fi

echo "=== Préparation des références Silver ==="

"$PYTHON" "$PROJECT_ROOT/scripts/prepare_real_references.py" \
  "$GTFS_ARCHIVE" \
  "$REFERENCE_DIR"

echo "=== Téléchargement GTFS-RT avec Java ==="

mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsRealtimeHttpDownloader \
  -Dexec.args="\"$REALTIME_FILE\""

echo "=== Normalisation et export JSONL ==="

mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=fr.remyqueny.sncf.producer.GtfsRealtimeJsonExporter \
  -Dexec.args="\"$REALTIME_FILE\" \"$GTFS_ARCHIVE\" \"$JSONL_FILE\""

EVENT_COUNT="$(wc -l < "$JSONL_FILE" | tr -d '[:space:]')"

if [[ "$EVENT_COUNT" -eq 0 ]]; then
  echo "Erreur : aucun événement exporté. Examiner le bilan des exclusions."
  exit 1
fi

echo "=== Collecte et préparation terminées ==="
printf 'Archive GTFS utilisée : %s\n' "$GTFS_ARCHIVE"
printf 'Instantané brut : %s\n' "$REALTIME_FILE"
printf 'Export canonique : %s\n' "$JSONL_FILE"
printf 'Références Silver : %s\n' "$REFERENCE_DIR"
printf 'Manifeste des références : %s/manifest.json\n' "$REFERENCE_DIR"
printf 'Événements exportés : %s\n' "$EVENT_COUNT"
echo "Aucun envoi Kafka et aucun traitement Spark."