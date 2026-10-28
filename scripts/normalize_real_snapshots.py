import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def atomic_json(path, content):
    descriptor, temporary = tempfile.mkstemp(
        prefix=".normalization-", suffix=".partial", dir=path.parent
    )
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            json.dump(content, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def validate_export(path, published_at):
    seen = set()
    with path.open(encoding="utf-8") as file:
        for line in file:
            event = json.loads(line)
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id.strip() or event_id in seen:
                raise ValueError(f"event_id absent ou dupliqué dans {path}")
            if event.get("source") != "sncf_gtfs_rt":
                raise ValueError(f"Source incorrecte dans {path}")
            if event.get("event_type") != "trip_update":
                raise ValueError(f"Type d'événement incorrect dans {path}")
            if type(event.get("schema_version")) is not int or event["schema_version"] != 1:
                raise ValueError(f"Version du contrat incorrecte dans {path}")
            if event.get("event_timestamp") != published_at:
                raise ValueError(f"Timestamp source incorrect dans {path}")
            for field in ("trip_id", "route_id", "stop_id"):
                value = event.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{field} absent dans {path}")
            seen.add(event_id)
    if not seen:
        raise ValueError(f"Export vide : {path}")
    return len(seen)


def normalize_pending(collection_root, gtfs_archive, reference_root, exporter):
    root = Path(collection_root).resolve()
    archive = Path(gtfs_archive).resolve()
    references = Path(reference_root).resolve()
    if not root.is_dir():
        raise ValueError(f"Dossier de collecte absent : {root}")

    with (root / ".normalization.lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return normalize_locked(root, archive, references, exporter)


def normalize_locked(root, archive, references, exporter):
    archive_hash = sha256(archive)
    reference_hashes = {
        name: sha256(references / name)
        for name in ("routes.csv", "stops.csv", "manifest.json")
    }
    reference_manifest = read_json(references / "manifest.json")
    if reference_manifest.get("source") != "sncf_gtfs_static":
        raise ValueError("Le référentiel n'est pas identifié comme SNCF réel.")
    if reference_manifest.get("archive_sha256") != archive_hash:
        raise ValueError("Archive GTFS différente des références préparées.")

    manifest_path = root / "normalization_manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
        expected = {
            "manifest_version": 1,
            "stage": "normalized",
            "collection_root": str(root),
            "gtfs_archive_path": str(archive),
            "gtfs_archive_sha256": archive_hash,
            "reference_root": str(references),
            "reference_files_sha256": reference_hashes,
        }
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise ValueError("Configuration ou références différentes du manifeste existant.")
    else:
        manifest = {
            "manifest_version": 1,
            "stage": "normalized",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "collection_root": str(root),
            "gtfs_archive_path": str(archive),
            "gtfs_archive_sha256": archive_hash,
            "reference_root": str(references),
            "reference_files_sha256": reference_hashes,
            "snapshots": [],
            "total_events": 0,
        }

    records = {}
    for record in manifest["snapshots"]:
        key = record["snapshot_sha256"]
        if key in records:
            raise ValueError("Instantané dupliqué dans le manifeste.")
        records[key] = record
    if manifest["total_events"] != sum(record["event_count"] for record in records.values()):
        raise ValueError("Total incohérent dans le manifeste.")

    entries = []
    for path in sorted((root / "registry").glob("*.json")):
        entry = read_json(path)
        key = entry["sha256"]
        if not re.fullmatch(r"[0-9a-f]{64}", key) or path.stem != key:
            raise ValueError(f"Empreinte de registre invalide : {path}")
        entries.append(entry)
    entries.sort(key=lambda entry: (entry["publishedAt"], entry["sha256"]))
    if not entries:
        raise ValueError("Aucun instantané enregistré dans le registre.")
    if not set(records).issubset({entry["sha256"] for entry in entries}):
        raise ValueError("Le manifeste référence un instantané absent du registre.")

    newly_normalized = 0
    already_normalized = 0
    quarantined = 0

    for entry in entries:
        snapshot = Path(entry["snapshotPath"]).resolve()
        if not snapshot.is_relative_to(root / "snapshots"):
            raise ValueError(f"Instantané hors du dossier de collecte : {snapshot}")
        if sha256(snapshot) != entry["sha256"]:
            raise ValueError(f"Instantané modifié : {snapshot}")
        if snapshot.stat().st_size != entry["sizeBytes"]:
            raise ValueError(f"Taille d'instantané incohérente : {snapshot}")

        output = snapshot.with_name("tripupdates.canonical.jsonl")
        partial = output.with_name(output.name + ".partial")
        intent_path = snapshot.with_name("normalization.intent.json")
        record = records.get(entry["sha256"])

        if record is not None:
            if (record["snapshot_path"] != str(snapshot)
                    or record["published_at"] != entry["publishedAt"]
                    or record["jsonl_path"] != str(output)
                    or type(record["event_count"]) is not int
                    or record["event_count"] <= 0
                    or sha256(output) != record["jsonl_sha256"]):
                raise ValueError(f"Export validé modifié ou métadonnées incohérentes : {output}")
            if partial.exists():
                raise ValueError(f"Fichier partiel inattendu pour un export validé : {partial}")
            already_normalized += 1
            continue

        intent = {
            "intent_version": 1,
            "snapshot_path": str(snapshot),
            "snapshot_sha256": entry["sha256"],
            "published_at": entry["publishedAt"],
            "jsonl_path": str(output),
            "gtfs_archive_path": str(archive),
            "gtfs_archive_sha256": archive_hash,
            "reference_root": str(references),
            "reference_files_sha256": reference_hashes,
        }

        if intent_path.exists():
            if read_json(intent_path) != intent:
                raise ValueError(f"Intention de normalisation incompatible : {intent_path}")
        else:
            if output.exists() or partial.exists():
                raise ValueError(f"Export sans provenance de normalisation : {output}")
            atomic_json(intent_path, intent)

        if output.exists() and partial.exists():
            raise ValueError(f"Sortie complète et partielle présentes simultanément : {output}")
        if partial.exists():
            if partial.is_symlink():
                raise ValueError(f"Lien symbolique inattendu : {partial}")
            quarantine = partial.with_name(
                partial.name + "." + uuid4().hex + ".abandoned"
            )
            partial.rename(quarantine)
            quarantined += 1

        if not output.exists():
            exporter(snapshot, archive, output)
        if partial.exists():
            raise ValueError(f"L'export a laissé un fichier partiel : {partial}")

        count = validate_export(output, entry["publishedAt"])
        if sha256(snapshot) != entry["sha256"] or sha256(archive) != archive_hash:
            raise ValueError("Une source a changé pendant la normalisation.")
        if any(sha256(references / name) != value for name, value in reference_hashes.items()):
            raise ValueError("Les références ont changé pendant la normalisation.")

        records[entry["sha256"]] = {
            "snapshot_path": str(snapshot),
            "snapshot_sha256": entry["sha256"],
            "published_at": entry["publishedAt"],
            "jsonl_path": str(output),
            "jsonl_sha256": sha256(output),
            "event_count": count,
        }
        manifest["snapshots"] = sorted(
            records.values(), key=lambda record: (record["published_at"], record["snapshot_sha256"])
        )
        manifest["total_events"] = sum(record["event_count"] for record in records.values())
        manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(manifest_path, manifest)
        newly_normalized += 1

    return {
        "newly_normalized": newly_normalized,
        "already_normalized": already_normalized,
        "indexed_snapshots": len(records),
        "indexed_events": manifest["total_events"],
        "quarantined_partials": quarantined,
        "manifest": str(manifest_path),
    }


def java_exporter(project_root):
    def export(snapshot, archive, output):
        java_home = os.environ.get("JAVA_HOME")
        java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
        if not java:
            raise ValueError("Java introuvable.")
        dependencies = (project_root / "producer-java/target/collector-classpath.txt").read_text(
            encoding="utf-8"
        ).strip()
        classpath = str(project_root / "producer-java/target/classes") + os.pathsep + dependencies
        subprocess.run([
            java, "-cp", classpath,
            "fr.remyqueny.sncf.producer.GtfsRealtimeJsonExporter",
            str(snapshot), str(archive), str(output),
        ], cwd=project_root, check=True)
    return export


def main():
    parser = argparse.ArgumentParser(description="Normaliser les instantanés enregistrés, sans Kafka.")
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("gtfs_archive", type=Path)
    parser.add_argument("reference_root", type=Path)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    try:
        result = normalize_pending(
            args.collection_root, args.gtfs_archive, args.reference_root, java_exporter(project)
        )
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exception:
        print(f"status=normalization_failed exception={type(exception).__name__} message={exception}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())