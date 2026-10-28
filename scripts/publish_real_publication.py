import argparse
import json
import os
import shutil
import signal
import sys
from collections import Counter
from pathlib import Path

from publication_state import PublicationRegistry, REAL_TOPIC, sha256
from run_active_real_collection import StopRequested, run_java


MAIN_CLASS = "fr.remyqueny.sncf.producer.RealJsonlProducer"


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def registry_for(collection_root):
    root = Path(collection_root).resolve()
    directory = root / "publication"
    if not directory.is_dir() or directory.resolve().parent != root:
        raise ValueError("Registre de publication absent ou hors de la collecte.")
    return root, PublicationRegistry(directory)


def validate_context(root, state):
    metadata = state["metadata"]
    manifest = read_json(root / "normalization_manifest.json")
    if (manifest.get("manifest_version") != 1
            or manifest.get("stage") != "normalized"
            or manifest.get("collection_root") != str(root)
            or metadata.get("collection_root") != str(root)):
        raise ValueError("Contexte de collecte incompatible avec le lot.")
    matches = [
        item for item in manifest["snapshots"]
        if item["snapshot_sha256"] == metadata["snapshot_sha256"]
    ]
    if len(matches) != 1:
        raise ValueError("Instantané du lot absent ou dupliqué dans la normalisation.")
    record = matches[0]
    for key in ("snapshot_path", "snapshot_sha256", "published_at", "jsonl_path", "jsonl_sha256"):
        if record[key] != metadata.get(key):
            raise ValueError(f"Provenance du lot incohérente : {key}")
    snapshot = Path(record["snapshot_path"]).resolve()
    if not snapshot.is_relative_to(root / "snapshots") or sha256(snapshot) != record["snapshot_sha256"]:
        raise ValueError("Instantané brut absent, modifié ou hors de la collecte.")
    if Path(metadata["jsonl_path"]).resolve() != snapshot.with_name("tripupdates.canonical.jsonl"):
        raise ValueError("Export JSONL hors de l'instantané attendu.")
    limit = metadata.get("selection_limit")
    if (type(limit) is not int or not 1 <= limit <= 20000
            or metadata.get("available_events") != record["event_count"]
            or metadata["event_count"] != min(limit, record["event_count"])):
        raise ValueError("Sélection du lot incohérente.")
    references = Path(manifest["reference_root"])
    hashes = manifest["reference_files_sha256"]
    if set(hashes) != {"routes.csv", "stops.csv", "manifest.json"}:
        raise ValueError("Liste des références incompatible.")
    if (str(references) != metadata.get("reference_root")
            or hashes["manifest.json"] != metadata.get("reference_manifest_sha256")
            or manifest["gtfs_archive_sha256"] != metadata.get("gtfs_archive_sha256")):
        raise ValueError("Référentiel du lot incohérent.")
    if any(sha256(references / name) != expected for name, expected in hashes.items()):
        raise ValueError("Références modifiées depuis la préparation.")
    if sha256(manifest["gtfs_archive_path"]) != manifest["gtfs_archive_sha256"]:
        raise ValueError("Archive GTFS modifiée depuis la préparation.")


def inspect_batches(collection_root, batch_id=None):
    root, registry = registry_for(collection_root)
    batch_ids = [batch_id] if batch_id is not None else [
        path.stem for path in sorted(registry.directory.glob("*.json"))
    ]
    if not batch_ids:
        raise ValueError("Aucun lot de publication préparé.")
    batches = []
    for identifier in batch_ids:
        state = registry.get(identifier)
        validate_context(root, state)
        metadata = state["metadata"]
        batches.append({
            "batch_id": identifier,
            "status": state["status"],
            "event_count": metadata["event_count"],
            "published_at": metadata["published_at"],
            "topic": metadata["topic"],
            "bootstrap_servers": metadata["bootstrap_servers"],
        })
    counts = Counter(item["status"] for item in batches)
    return {
        "mode": "inspect", "kafka_send_performed": False,
        "selected_events": sum(item["event_count"] for item in batches),
        "pending_batches": counts["pending"],
        "publishing_batches": counts["publishing"],
        "published_batches": counts["published"],
        "uncertain_batches": counts["uncertain"],
        "batches": batches,
    }


def java_prefix(project):
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
    if not java:
        raise ValueError("Java introuvable.")
    classes = project / "producer-java/target/classes"
    if not (classes / (MAIN_CLASS.replace(".", "/") + ".class")).is_file():
        raise ValueError("Producteur Java non compilé.")
    dependencies = (project / "producer-java/target/collector-classpath.txt").read_text(encoding="utf-8").strip()
    if not dependencies:
        raise ValueError("Classpath Java vide.")
    return [java, "-cp", str(classes) + os.pathsep + dependencies]


def make_java_sender(project, receipt_path, log_path, runner=run_java, prefix=None):
    prefix = java_prefix(project) if prefix is None else list(prefix)

    def send(metadata):
        if metadata["topic"] != REAL_TOPIC:
            raise ValueError("Topic réel obligatoire.")
        command = prefix + [
            MAIN_CLASS,
            metadata["jsonl_path"], str(metadata["event_count"]),
            metadata["bootstrap_servers"], str(receipt_path), metadata["jsonl_sha256"],
        ]
        with log_path.open("x", encoding="utf-8") as log:
            runner(command, project, stdout=log)
        return read_json(receipt_path)

    return send


def publish_one(collection_root, batch_id, project_root, sender_factory=None):
    root, registry = registry_for(collection_root)
    state = registry.get(batch_id)
    validate_context(root, state)
    receipt_path = registry.directory / "receipts" / f"{batch_id}.json"
    log_path = receipt_path.with_suffix(".log")

    if state["status"] == "pending":
        partial = receipt_path.with_name(receipt_path.name + ".partial")
        if any(path.exists() or path.is_symlink() for path in (receipt_path, partial, log_path)):
            raise ValueError("Reçu, fichier partiel ou log déjà présent : diagnostic requis.")
        if sender_factory is None:
            sender = make_java_sender(Path(project_root).resolve(), receipt_path, log_path)
        else:
            sender = sender_factory(receipt_path, log_path)
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        def sender(metadata):
            raise RuntimeError("Émetteur appelé pour un lot qui n'est pas en attente.")

    result = registry.publish(batch_id, sender)
    final_state = result["state"]
    return {
        "mode": "execute", "batch_id": batch_id,
        "published_now": result["published_now"],
        "status": final_state["status"],
        "event_count": final_state["metadata"]["event_count"],
        "receipt_file": str(receipt_path) if receipt_path.is_file() else None,
        "producer_log": str(log_path) if log_path.is_file() else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Inspecter les lots SNCF ; publier un seul lot avec --execute et --batch-id.")
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("--batch-id")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute and not args.batch_id:
        parser.error("--execute exige un --batch-id explicite.")
    previous = {}

    def request_stop(signum, frame):
        raise StopRequested(signum)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
        if args.execute:
            result = publish_one(
                args.collection_root, args.batch_id, Path(__file__).resolve().parents[1]
            )
        else:
            result = inspect_batches(args.collection_root, args.batch_id)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except StopRequested as stop:
        print(f"status=publication_interrupted signal={signal.Signals(stop.signum).name}", file=sys.stderr, flush=True)
        return 128 + stop.signum
    except (Exception, KeyboardInterrupt) as exception:
        print(f"status=publication_failed exception={type(exception).__name__} message={exception}", file=sys.stderr, flush=True)
        return 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())