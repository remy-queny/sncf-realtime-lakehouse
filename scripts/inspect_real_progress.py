import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from progress_health import assess_stage, parse_timestamp


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_events(path):
    if not path.is_file():
        return []

    events = []
    with path.open(encoding="utf-8", errors="replace") as file:
        for line in file:
            text = line.strip()
            if not text.startswith("{"):
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                events.append(value)

    return events


def build_report(
    ingestion_events,
    gold_events,
    *,
    now,
    exit_code=None,
    collection_timeout=300,
    kafka_timeout=300,
    gold_timeout=180,
):
    ingestion_completed = any(
        event.get("status") == "session_completed"
        for event in ingestion_events
    )
    stopped = exit_code is not None
    ingestion_active = not stopped and not ingestion_completed

    stages = {}
    for name, events, started, completed, timeout, active in [
        (
            "collection", ingestion_events,
            "collection_started", "collection_completed",
            collection_timeout, ingestion_active,
        ),
        (
            "kafka", ingestion_events,
            "kafka_publication_started", "kafka_publication_completed",
            kafka_timeout, ingestion_active,
        ),
        (
            "gold", gold_events,
            "gold_check_started", "gold_check_completed",
            gold_timeout, not stopped,
        ),
    ]:
        stages[name] = assess_stage(
            events,
            started_stage=started,
            completed_stage=completed,
            now=now,
            timeout_seconds=timeout,
            expected_active=active,
        )

    collections = [
        event for event in ingestion_events
        if event.get("stage") == "collection_completed"
    ]
    source_observation = "unknown"

    if collections:
        latest = max(
            collections,
            key=lambda event: parse_timestamp(event["timestamp"]),
        )
        count = latest.get("new_snapshot_count")
        if type(count) is int and count >= 0:
            source_observation = (
                "no_new_snapshot" if count == 0 else "new_snapshot"
            )

    return {
        "checked_at": now,
        "session_state": (
            "stopped" if exit_code == 0
            else "failed" if stopped
            else "not_confirmed_stopped"
        ),
        "exit_code": exit_code,
        "ingestion_completed": ingestion_completed,
        "last_collection_observation": source_observation,
        "stages": stages,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Inspecter la progression réelle sans Spark."
    )
    parser.add_argument("session", type=Path)
    parser.add_argument("--collection-timeout", type=int, default=300)
    parser.add_argument("--kafka-timeout", type=int, default=300)
    parser.add_argument("--gold-timeout", type=int, default=180)
    args = parser.parse_args()

    session = args.session.resolve()
    allowed = (PROJECT_ROOT / "data/source_checks").resolve()

    if not session.is_relative_to(allowed) or not session.is_dir():
        parser.error("Dossier de session existant requis dans data/source_checks.")

    exit_code = None
    exit_path = session / "exit-code.json"
    if exit_path.exists():
        state = json.loads(exit_path.read_text(encoding="utf-8"))
        exit_code = state.get("exit_code")
        if type(exit_code) is not int:
            raise ValueError("Code de sortie de session invalide.")

    report = build_report(
        read_events(session / "logs/ingestion.log"),
        read_events(session / "logs/gold.log"),
        now=datetime.now(timezone.utc).isoformat(),
        exit_code=exit_code,
        collection_timeout=args.collection_timeout,
        kafka_timeout=args.kafka_timeout,
        gold_timeout=args.gold_timeout,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()