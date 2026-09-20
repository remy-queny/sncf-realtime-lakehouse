import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer


TOPIC = os.getenv("KAFKA_TRIP_UPDATES_TOPIC", "sncf.trip_updates.raw")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SIMULATED_TRIPS = [
    {
        "trip_id": "TRAIN-H-001",
        "route_id": "H",
        "stop_id": "stop_87271007",
        "delay_seconds": 480,
    },
    {
        "trip_id": "TRAIN-J-002",
        "route_id": "J",
        "stop_id": "stop_87271003",
        "delay_seconds": 720,
    },
    {
        "trip_id": "TRAIN-R-003",
        "route_id": "R",
        "stop_id": "stop_87271005",
        "delay_seconds": 120,
    },
    {
        "trip_id": "TRAIN-L-004",
        "route_id": "L",
        "stop_id": "stop_87113001",
        "delay_seconds": 600,
    },
    {
        "trip_id": "TRAIN-H-005",
        "route_id": "H",
        "stop_id": "stop_87271010",
        "delay_seconds": 0,
    },
]


def build_event() -> dict:
    trip = random.choice(SIMULATED_TRIPS)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    scheduled_timestamp = now + timedelta(minutes=5)
    estimated_timestamp = scheduled_timestamp + timedelta(
        seconds=trip["delay_seconds"]
    )

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "trip_update",
        "source": "simulation",
        "schema_version": 1,
        "trip_id": trip["trip_id"],
        "route_id": trip["route_id"],
        "stop_id": trip["stop_id"],
        "scheduled_timestamp": scheduled_timestamp.isoformat().replace("+00:00", "Z"),
        "estimated_timestamp": estimated_timestamp.isoformat().replace("+00:00", "Z"),
        "delay_seconds": trip["delay_seconds"],
        "event_timestamp": now.isoformat().replace("+00:00", "Z"),
        "ingested_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Delivery failed: {error}", file=sys.stderr)
        return

    print(
        f"Delivered event_id={message.key().decode('utf-8')} "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def main() -> None:
    print(f"Connecting to Kafka at: {BOOTSTRAP_SERVERS}")
    print(f"Publishing to topic: {TOPIC}")
    parser = argparse.ArgumentParser(
        description="Publish simulated SNCF trip-update events to Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of simulated events to publish. Default: 10.",
    )
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "sncf-trip-update-simulator",
            "acks": "all",
        }
    )

    for _ in range(args.count):
        event = build_event()
        key = event["trip_id"]

        producer.produce(
            topic=TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    undelivered_messages = producer.flush(10)

    if undelivered_messages > 0:
        raise RuntimeError(
            f"{undelivered_messages} message(s) were not delivered to Kafka."
        )


if __name__ == "__main__":
    main()