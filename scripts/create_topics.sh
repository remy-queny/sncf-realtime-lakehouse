#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

KAFKA_BOOTSTRAP_SERVER="kafka:29092"

create_topic() {
  local topic_name="$1"

  echo "=== Préparation du topic : $topic_name ==="

  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic_name" \
    --partitions 1 \
    --replication-factor 1

  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --describe \
    --topic "$topic_name"

  echo "Topic ready: $topic_name"
}

create_topic "sncf.trip_updates.raw"