import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "spark/src"))

from publication_state import PublicationRegistry
from publish_real_publication import validate_context
from jobs.gold_cycle import read_silver_snapshot
from jobs.stream_silver import create_spark_session
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Vérifier la présence des lots réels dans une version Silver."
    )
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("--silver-version", type=int, required=True)
    args = parser.parse_args()

    if args.silver_version < 0:
        parser.error("--silver-version doit être positif ou nul.")

    root = args.collection_root.resolve()
    allowed_root = (PROJECT_ROOT / "data/source_checks").resolve()

    if not root.is_relative_to(allowed_root):
        parser.error("La collecte doit être située dans data/source_checks.")

    publication_root = root / "publication"
    if not publication_root.is_dir():
        parser.error("Registre de publication absent.")

    registry = PublicationRegistry(publication_root)
    batches = []
    expected_ids = set()

    for path in sorted(publication_root.glob("*.json")):
        state = registry.get(path.stem)
        validate_context(root, state)

        if state["status"] != "published":
            raise ValueError(
                f"Lot non terminé : {state['batch_id']} "
                f"({state['status']})."
            )

        ids = {
            ack["event_id"]
            for ack in state["receipt"]["acknowledgements"]
        }

        batches.append((state["batch_id"], ids))
        expected_ids.update(ids)

    if not batches:
        raise ValueError("Aucun lot trouvé.")

    if len(expected_ids) > 20_000:
        raise ValueError(
            "Contrôle local limité à 20000 identifiants distincts."
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        silver = read_silver_snapshot(
            spark,
            PROJECT_ROOT / "data/lakehouse_real/silver/trip_delays",
            args.silver_version,
        )

        rows = (
            silver
            .filter(F.col("event_id").isin(sorted(expected_ids)))
            .select("event_id")
            .limit(20_001)
            .collect()
        )

        if len(rows) > 20_000:
            raise ValueError("Trop de lignes correspondantes dans Silver.")

        present_ids = {row["event_id"] for row in rows}

        if len(rows) != len(present_ids):
            raise ValueError(
                "Doublons Silver détectés parmi les identifiants contrôlés."
            )

        results = []
        for batch_id, ids in batches:
            matched = len(ids & present_ids)
            results.append({
                "batch_id": batch_id,
                "expected_event_count": len(ids),
                "matched_event_count": matched,
                "complete": matched == len(ids),
            })

        report = {
            "collection_root": str(root),
            "silver_version": args.silver_version,
            "expected_distinct_event_count": len(expected_ids),
            "matched_distinct_event_count": len(present_ids),
            "all_batches_complete": all(
                item["complete"] for item in results
            ),
            "batches": results,
        }

        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["all_batches_complete"] else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())