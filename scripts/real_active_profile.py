import csv
import json
import os
import sys
from pathlib import Path


def build_real_environment(
    project_root,
    reference_root,
    interval=30,
    base=None,
):
    project = Path(project_root).resolve()
    references = Path(reference_root)
    if not references.is_absolute():
        references = project / references
    references = references.resolve()

    if type(interval) is not int or interval <= 0:
        raise ValueError("Intervalle entier strictement positif requis.")

    allowed_roots = (
        project / "data/reference/real",
        project / "data/source_checks",
    )
    if not any(
        references.is_relative_to(root)
        for root in allowed_roots
    ):
        raise ValueError("Références hors du périmètre réel autorisé.")

    manifest = json.loads(
        (references / "manifest.json").read_text(encoding="utf-8")
    )
    if manifest.get("source") != "sncf_gtfs_static":
        raise ValueError("Un référentiel SNCF réel est requis.")

    for filename, required in (
        ("routes.csv", {"route_id", "line_name"}),
        ("stops.csv", {"stop_id", "stop_name"}),
    ):
        with (references / filename).open(
            encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.DictReader(file)
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(f"Colonnes absentes : {filename}")

    lakehouse = project / "data/lakehouse_real"
    checkpoints = project / "data/checkpoints_real"

    env = dict(os.environ if base is None else base)
    env.update({
        "PYTHONPATH": str(project / "spark/src"),
        "PYSPARK_PYTHON": sys.executable,
        "PYTHONUNBUFFERED": "1",
        "SPARK_MASTER": "local[2]",
        "STREAMING_MODE": "active",
        "STREAMING_INTERVAL_SECONDS": str(interval),
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_TRIP_UPDATES_TOPIC": "sncf.trip_updates.real.raw",
        "LAKEHOUSE_ROOT": str(lakehouse),
        "CHECKPOINT_ROOT": str(checkpoints),
        "GTFS_REFERENCE_ROOT": str(references),
        "BRONZE_TRIP_UPDATES_PATH": str(
            lakehouse / "bronze/trip_updates"
        ),
        "BRONZE_TRIP_UPDATES_CHECKPOINT_PATH": str(
            checkpoints / "bronze_trip_updates"
        ),
        "POSTGRES_TARGET_SCHEMA": "sncf_real",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
    })
    return env