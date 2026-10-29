import json
import sys
from pathlib import Path

from run_active_real_collection import session_configuration
from run_active_real_ingestion import validate_java


def load_ingestion_template(
    project_root,
    collection_root,
    reference_root,
):
    project = Path(project_root).resolve()
    collection = Path(collection_root)
    if not collection.is_absolute():
        collection = project / collection
    collection = collection.resolve()

    if not collection.is_relative_to(project / "data/source_checks"):
        raise ValueError("Collecte modèle hors du périmètre autorisé.")

    config = json.loads(
        (collection / "collection_config.json").read_text(
            encoding="utf-8"
        )
    )

    checked = session_configuration(
        config["source_url"],
        Path(config["gtfs_archive_path"]),
        Path(config["reference_root"]),
    )

    if checked != config:
        raise ValueError("La configuration ou les références ont changé.")

    if Path(config["reference_root"]).resolve() != (
        Path(reference_root).resolve()
    ):
        raise ValueError(
            "L'ingestion et Silver doivent utiliser les mêmes références."
        )

    validate_java(project)
    return checked


def build_ingestion_command(
    project_root,
    config,
    collection_root,
    *,
    interval=120,
    cycles=3,
    limit=100,
):
    for value in (interval, cycles, limit):
        if type(value) is not int or value <= 0:
            raise ValueError("Paramètres entiers strictement positifs requis.")

    if limit > 20_000:
        raise ValueError("La limite ne doit pas dépasser 20000.")

    project = Path(project_root).resolve()
    collection = Path(collection_root).resolve()

    if collection.exists():
        raise ValueError("Une nouvelle collecte inexistante est requise.")

    return [
        sys.executable,
        str(project / "scripts/run_active_real_ingestion.py"),
        config["source_url"],
        str(collection),
        config["gtfs_archive_path"],
        config["reference_root"],
        "--interval", str(interval),
        "--cycles", str(cycles),
        "--limit", str(limit),
        "--bootstrap-servers", "localhost:9092",
        "--execute",
    ]