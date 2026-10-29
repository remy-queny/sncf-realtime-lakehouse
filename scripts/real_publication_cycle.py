from pathlib import Path

from prepare_real_publication import prepare_publications
from publication_state import UncertainPublication
from publish_real_publication import inspect_batches, publish_one

import time

from normalize_real_snapshots import normalize_pending
from run_active_real_collection import run_session

def run_publication_cycle(
    collection_root,
    project_root,
    *,
    limit=100,
    bootstrap_servers="localhost:9092",
):
    root = Path(collection_root).resolve()
    project = Path(project_root).resolve()

    preparation = prepare_publications(
        root,
        limit=limit,
        bootstrap_servers=bootstrap_servers,
    )

    inspection = inspect_batches(root)
    batches = inspection["batches"]

    blocked = [
        batch["batch_id"]
        for batch in batches
        if batch["status"] in {"publishing", "uncertain"}
    ]
    if blocked:
        raise UncertainPublication(
            "Diagnostic requis avant tout nouvel envoi : "
            + ", ".join(blocked)
        )

    batches = sorted(
        batches,
        key=lambda batch: (batch["published_at"], batch["batch_id"]),
    )

    publications = []
    skipped = 0

    for batch in batches:
        if batch["status"] == "published":
            skipped += 1
            continue

        if batch["status"] != "pending":
            raise ValueError(f"Statut inattendu : {batch['status']}")

        result = publish_one(root, batch["batch_id"], project)
        publications.append(result)

    return {
        "stage": "publication_cycle_completed",
        "preparation": preparation,
        "published_batches": sum(
            result["published_now"] for result in publications
        ),
        "published_events": sum(
            result["event_count"]
            for result in publications
            if result["published_now"]
        ),
        "skipped_published_batches": skipped,
        "publications": publications,
    }

def run_publishing_session(
    source,
    collection_root,
    gtfs_archive,
    reference_root,
    *,
    project_root,
    collect,
    exporter,
    interval=120,
    cycles=3,
    limit=100,
    bootstrap_servers="localhost:9092",
    waiter=time.sleep,
    reporter=print,
):
    root = Path(collection_root).resolve()

    if root.exists():
        raise ValueError(
            "Cette première version exige un nouveau dossier de collecte "
            "inexistant pour ne pas publier d'anciens lots en attente."
        )

    if type(limit) is not int or not 1 <= limit <= 20_000:
        raise ValueError("La limite doit être comprise entre 1 et 20000.")

    published_batches = 0
    published_events = 0

    def normalize_and_publish(root, archive, references, export):
        nonlocal published_batches, published_events

        normalization = normalize_pending(
            root, archive, references, export
        )

        publication = run_publication_cycle(
            root,
            project_root,
            limit=limit,
            bootstrap_servers=bootstrap_servers,
        )

        published_batches += publication["published_batches"]
        published_events += publication["published_events"]

        return {
            **normalization,
            "publication": publication,
        }

    result = run_session(
        source,
        root,
        gtfs_archive,
        reference_root,
        interval,
        cycles,
        collect,
        exporter,
        waiter=waiter,
        reporter=reporter,
        normalizer=normalize_and_publish,
    )

    return {
        **result,
        "kafka_enabled": True,
        "published_batches": published_batches,
        "published_events": published_events,
    }