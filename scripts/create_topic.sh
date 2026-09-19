#!/usr/bin/env bash

set -euo pipefail

KAFKA_CONTAINER="sncf-kafka"
KAFKA_BOOTSTRAP_SERVER="kafka:29092"

create_topic() {
  local topic_name="$1"

  if docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --list | grep -qx "$topic_name"; then

    echo "Topic already exists: $topic_name"
  else
    docker exec "$KAFKA_CONTAINER" \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
      --create \
      --topic "$topic_name" \
      --partitions 1 \
      --replication-factor 1

    echo "Topic created: $topic_name"
  fi
}

create_topic "sncf.trip_updates.raw"