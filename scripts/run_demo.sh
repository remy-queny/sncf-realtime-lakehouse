#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Vérification du topic Kafka ==="
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic sncf.trip_updates.raw

echo "=== Production d'un événement simulé ==="
(
  cd producer-java
  SIMULATION_MODE=true mvn compile exec:java \
    -Dexec.mainClass=fr.remyqueny.sncf.producer.ProducerApplication
)

echo "=== Traitement et publication ==="
bash "$PROJECT_ROOT/scripts/run_pipeline.sh"

echo "=== Démonstration terminée ==="
echo "Actualise le dashboard Metabase pour consulter les résultats."