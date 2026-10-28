import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from publication_state import (
    MAX_EVENTS,
    REAL_TOPIC,
    PublicationRegistry,
    batch_identity,
    sha256,
    validate_metadata,
)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def prepare_publications(collection_root, limit=100, bootstrap_servers="localhost:9092"):
    if type(limit) is not int or not 1 <= limit <= MAX_EVENTS:
        raise ValueError(f"La limite doit être comprise entre 1 et {MAX_EVENTS}.")
    if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
        raise ValueError("Adresse bootstrap absente.")
    root = Path(collection_root).resolve()
    manifest = read_json(root / "normalization_manifest.json")
    if (manifest.get("manifest_version") != 1
            or manifest.get("stage") != "normalized"
            or manifest.get("collection_root") != str(root)):
        raise ValueError("Manifeste de normalisation incompatible.")

    references = Path(manifest["reference_root"])
    archive = Path(manifest["gtfs_archive_path"])
    reference_hashes = manifest["reference_files_sha256"]
    if set(reference_hashes) != {"routes.csv", "stops.csv", "manifest.json"}:
        raise ValueError("Liste des références incompatible.")
    for name, expected in reference_hashes.items():
        if sha256(references / name) != expected:
            raise ValueError(f"Référence modifiée : {name}")
    archive_hash = sha256(archive)
    reference_manifest = read_json(references / "manifest.json")
    if (archive_hash != manifest["gtfs_archive_sha256"]
            or reference_manifest.get("archive_sha256") != archive_hash
            or reference_manifest.get("source") != "sncf_gtfs_static"):
        raise ValueError("Archive GTFS différente du référentiel validé.")

    snapshots = manifest["snapshots"]
    if not isinstance(snapshots, list) or not snapshots:
        raise ValueError("Aucun instantané normalisé disponible.")
    if any(type(item.get("event_count")) is not int or item["event_count"] <= 0 for item in snapshots):
        raise ValueError("Nombre d'observations normalisées invalide.")
    total_available = sum(item["event_count"] for item in snapshots)
    if manifest["total_events"] != total_available:
        raise ValueError("Total incohérent dans le manifeste de normalisation.")

    metadata_list = []
    seen = set()
    for record in sorted(snapshots, key=lambda item: (item["published_at"], item["snapshot_sha256"])):
        key = record["snapshot_sha256"]
        if key in seen:
            raise ValueError("Instantané dupliqué dans le manifeste.")
        seen.add(key)
        snapshot = Path(record["snapshot_path"]).resolve()
        output = Path(record["jsonl_path"]).resolve()
        if not snapshot.is_relative_to(root / "snapshots"):
            raise ValueError("Instantané situé hors du dossier de collecte.")
        if output != snapshot.with_name("tripupdates.canonical.jsonl"):
            raise ValueError("Chemin JSONL incompatible avec l'instantané.")
        if sha256(snapshot) != key:
            raise ValueError("Instantané brut modifié.")
        registry_entry = read_json(root / "registry" / f"{key}.json")
        if (registry_entry.get("sha256") != key
                or Path(registry_entry["snapshotPath"]).resolve() != snapshot
                or registry_entry.get("publishedAt") != record["published_at"]
                or registry_entry.get("sizeBytes") != snapshot.stat().st_size):
            raise ValueError("Registre de collecte incompatible avec la normalisation.")
        if sha256(output) != record["jsonl_sha256"]:
            raise ValueError("Export JSONL modifié.")

        metadata = {
            "collection_root": str(root),
            "snapshot_path": str(snapshot),
            "snapshot_sha256": key,
            "published_at": record["published_at"],
            "jsonl_path": str(output),
            "jsonl_sha256": record["jsonl_sha256"],
            "available_events": record["event_count"],
            "event_count": min(limit, record["event_count"]),
            "selection": "first_n",
            "selection_limit": limit,
            "topic": REAL_TOPIC,
            "bootstrap_servers": bootstrap_servers,
            "gtfs_archive_sha256": archive_hash,
            "reference_root": str(references),
            "reference_manifest_sha256": reference_hashes["manifest.json"],
        }
        validate_metadata(metadata)
        metadata_list.append(metadata)

    publication_root = (root / "publication").resolve()
    if publication_root.parent != root:
        raise ValueError("Le registre de publication doit rester dans la collecte.")
    registry = PublicationRegistry(publication_root)

    for metadata in metadata_list:
        batch_id = batch_identity(metadata)
        if registry.path(batch_id).exists():
            existing = registry.get(batch_id)
            if existing["metadata"] != metadata:
                raise ValueError("Lot existant : sélection ou provenance différente.")

    states = [registry.prepare(metadata) for metadata in metadata_list]
    statuses = Counter(state["status"] for state in states)
    return {
        "snapshots_considered": len(states),
        "available_events": total_available,
        "selected_events": sum(state["metadata"]["event_count"] for state in states),
        "selection": "first_n",
        "per_snapshot_limit": limit,
        "pending_batches": statuses["pending"],
        "publishing_batches": statuses["publishing"],
        "published_batches": statuses["published"],
        "uncertain_batches": statuses["uncertain"],
        "topic": REAL_TOPIC,
        "bootstrap_servers": bootstrap_servers,
        "registry": str(publication_root),
        "kafka_send_performed": False,
    }


def main():
    parser = argparse.ArgumentParser(description="Préparer des lots de publication SNCF, sans connexion Kafka.")
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()
    try:
        result = prepare_publications(args.collection_root, args.limit, args.bootstrap_servers)
    except (OSError, ValueError, KeyError, TypeError) as exception:
        print(f"status=publication_preparation_failed exception={type(exception).__name__} message={exception}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())