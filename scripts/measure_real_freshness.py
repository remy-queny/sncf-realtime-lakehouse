import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "spark/src"))

from publication_state import PublicationRegistry
from publish_real_publication import validate_context
from jobs.freshness import measure_batch_freshness
from jobs.gold_cycle import read_silver_snapshot
from jobs.stream_silver import create_spark_session
from pyspark.sql import functions as F


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_cycles(path):
    cycles = []

    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue

        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            continue

        if "silver_version" not in item:
            continue

        if not item.get("postgres_published_at"):
            raise ValueError(
                "Un cycle Gold ne contient pas postgres_published_at."
            )

        cycles.append(item)

    if not cycles:
        raise ValueError("Aucun cycle Gold horodaté trouvé.")

    return sorted(
        cycles,
        key=lambda item: datetime.fromisoformat(
            item["postgres_published_at"].replace("Z", "+00:00")
        ),
    )


def load_batches(collection):
    directory = collection / "publication"
    if not directory.is_dir():
        raise ValueError("Registre de publication absent.")

    registry = PublicationRegistry(directory)
    batches = []

    for path in sorted(directory.glob("*.json")):
        state = registry.get(path.stem)
        validate_context(collection, state)

        if state["status"] != "published":
            raise ValueError(f"Lot non terminé : {state['batch_id']}")

        metadata = state["metadata"]
        sha256 = metadata["snapshot_sha256"]
        snapshot = read_json(collection / "registry" / f"{sha256}.json")

        if (
            snapshot["sha256"] != sha256
            or snapshot["publishedAt"] != metadata["published_at"]
            or Path(snapshot["snapshotPath"]).resolve()
            != Path(metadata["snapshot_path"]).resolve()
        ):
            raise ValueError("Registres source/publication incohérents.")

        batches.append({
            "batch_id": state["batch_id"],
            "expected_event_ids": [
                ack["event_id"]
                for ack in state["receipt"]["acknowledgements"]
            ],
            "source_published_at": snapshot["publishedAt"],
            "collected_at": snapshot["collectedAt"],
        })

    if not batches:
        raise ValueError("Aucun lot trouvé.")

    return batches


def main():
    parser = argparse.ArgumentParser(
        description="Mesurer les délais des lots d'une session réelle arrêtée."
    )
    parser.add_argument("session", type=Path)
    args = parser.parse_args()

    session = args.session.resolve()
    allowed = (PROJECT_ROOT / "data/source_checks").resolve()

    if not session.is_relative_to(allowed):
        parser.error("Session hors de data/source_checks.")

    exit_state = read_json(session / "exit-code.json")
    if exit_state.get("exit_code") != 0:
        raise ValueError("La session doit être arrêtée avec exit_code=0.")

    output = session / "freshness_report.json"
    if output.exists():
        raise ValueError(f"Rapport déjà présent, non écrasé : {output}")

    batches = load_batches(session / "collection")
    cycles = load_cycles(session / "logs/gold.log")

    expected_ids = {
        event_id
        for batch in batches
        for event_id in batch["expected_event_ids"]
    }
    if len(expected_ids) > 20_000:
        raise ValueError("Analyse locale limitée à 20000 identifiants.")

    measurements = {}
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        for cycle in cycles:
            version = cycle["silver_version"]
            silver = read_silver_snapshot(
                spark,
                PROJECT_ROOT / "data/lakehouse_real/silver/trip_delays",
                version,
            )

            rows = (
                silver
                .filter(F.col("event_id").isin(sorted(expected_ids)))
                .select("event_id")
                .limit(20_001)
                .collect()
            )
            present_ids = {row["event_id"] for row in rows}

            if len(rows) > 20_000 or len(rows) != len(present_ids):
                raise ValueError("Volume excessif ou doublons Silver.")

            for batch in batches:
                identifier = batch["batch_id"]
                if identifier in measurements:
                    continue

                result = measure_batch_freshness(
                    **batch,
                    present_event_ids=present_ids,
                    postgres_published_at=cycle["postgres_published_at"],
                )

                if result["status"] == "complete":
                    measurements[identifier] = {
                        **result,
                        "silver_version": version,
                        "source_published_at": batch["source_published_at"],
                        "collected_at": batch["collected_at"],
                        "postgres_published_at": cycle["postgres_published_at"],
                    }

            if len(measurements) == len(batches):
                break
    finally:
        spark.stop()

    missing = [
        batch["batch_id"]
        for batch in batches
        if batch["batch_id"] not in measurements
    ]
    report = {
        "session": str(session),
        "measurement_basis": "first_complete_publication_in_session_log",
        "all_batches_measured": not missing,
        "unmeasured_batch_ids": missing,
        "batches": list(measurements.values()),
    }

    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Rapport enregistré : {output}")
    return 0 if not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())