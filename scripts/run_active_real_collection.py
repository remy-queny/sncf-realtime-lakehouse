import argparse
import fcntl
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit


RAW_MAIN = "fr.remyqueny.sncf.producer.GtfsRealtimeCollectorApplication"
INSPECTOR = "fr.remyqueny.sncf.producer.GtfsRealtimeEnrichmentInspector"
EXPORTER = "fr.remyqueny.sncf.producer.GtfsRealtimeJsonExporter"


class StopRequested(BaseException):
    def __init__(self, signum):
        self.signum = signum
        super().__init__(signal.Signals(signum).name)


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def atomic_json(path, content):
    descriptor, temporary = tempfile.mkstemp(
        prefix=".collection-", suffix=".partial", dir=path.parent
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


def session_configuration(source, archive, references):
    parsed = urlsplit(source)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Une URL HTTP ou HTTPS avec hôte est requise.")
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise ValueError("L'URL ne doit contenir ni identifiants intégrés ni fragment.")
    parsed.port
    reference_manifest = json.loads((references / "manifest.json").read_text(encoding="utf-8"))
    archive_hash = sha256(archive)
    if reference_manifest.get("source") != "sncf_gtfs_static":
        raise ValueError("Référentiel SNCF réel attendu.")
    if reference_manifest.get("archive_sha256") != archive_hash:
        raise ValueError("Archive GTFS différente des références préparées.")
    return {
        "config_version": 1,
        "source_url": source,
        "gtfs_archive_path": str(archive),
        "gtfs_archive_sha256": archive_hash,
        "reference_root": str(references),
        "reference_files_sha256": {
            name: sha256(references / name)
            for name in ("routes.csv", "stops.csv", "manifest.json")
        },
    }


def run_session(
    source, collection_root, gtfs_archive, reference_root,
    interval, cycles, collect, exporter,
    waiter=time.sleep, reporter=print, normalizer=None,
):
    if interval <= 0 or cycles <= 0:
        raise ValueError("Intervalle et nombre de cycles strictement positifs requis.")
    root = Path(collection_root).resolve()
    archive = Path(gtfs_archive).resolve()
    references = Path(reference_root).resolve()
    configuration = session_configuration(source, archive, references)
    if normalizer is None:
        from normalize_real_snapshots import normalize_pending
        normalizer = normalize_pending
    root.mkdir(parents=True, exist_ok=True)

    with (root / ".active-collection.lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        config_path = root / "collection_config.json"
        if config_path.exists():
            existing = json.loads(config_path.read_text(encoding="utf-8"))
            if existing != configuration:
                raise ValueError("Source ou référentiel différent de la session existante.")
        else:
            if any((root / "registry").glob("*.json")) or (root / "normalization_manifest.json").exists():
                raise ValueError("Ancien dossier sans configuration de session : utiliser un nouveau dossier.")
            atomic_json(config_path, configuration)

        total_new = 0
        latest = None
        if any((root / "registry").glob("*.json")):
            latest = normalizer(root, archive, references, exporter)
            total_new += latest["newly_normalized"]
            reporter(json.dumps({"stage": "resume", **latest}, ensure_ascii=False))

        for cycle in range(1, cycles + 1):
            if session_configuration(source, archive, references) != configuration:
                raise ValueError("Les références ont changé pendant la session.")
            collect(source, root)
            latest = normalizer(root, archive, references, exporter)
            total_new += latest["newly_normalized"]
            reporter(json.dumps({"stage": "cycle", "cycle": cycle, **latest}, ensure_ascii=False))
            if cycle < cycles:
                waiter(interval)

        return {
            "status": "session_completed",
            "completed_cycles": cycles,
            "newly_normalized": total_new,
            "indexed_snapshots": latest["indexed_snapshots"],
            "indexed_events": latest["indexed_events"],
            "kafka_enabled": False,
        }


def stop_owned_process(process):
    forced = False
    for signum, timeout in (
        (signal.SIGINT, 35), (signal.SIGTERM, 5), (signal.SIGKILL, 5)
    ):
        if process.poll() is not None:
            return forced
        try:
            process.send_signal(signum)
        except ProcessLookupError:
            process.wait(timeout=5)
            return forced
        try:
            process.wait(timeout=timeout)
            return forced
        except subprocess.TimeoutExpired:
            forced = True
    raise RuntimeError("Impossible de confirmer l'arrêt du processus Java lancé.")


def run_java(command, project_root, stdout=None):
    process = subprocess.Popen(
        command, cwd=project_root, start_new_session=True,
        stdout=stdout, stderr=subprocess.STDOUT,
    )
    try:
        code = process.wait()
        if code != 0:
            raise subprocess.CalledProcessError(code, command)
    finally:
        if process.poll() is None and stop_owned_process(process):
            raise RuntimeError("Arrêt Java forcé : la session est en échec.")


def check_reference_output(text):
    marker = "--- Compteurs ---"
    if marker not in text:
        raise ValueError("Sortie inattendue de l'inspecteur GTFS.")
    counts = {}
    for line in text.split(marker, 1)[1].splitlines():
        name, separator, value = line.partition(" : ")
        if separator:
            counts[name.strip()] = int(value.strip())
    planned = counts["courses_planifiees"]
    enriched = counts.get("courses_enrichies", 0)
    missing_trips = counts.get("courses_reference_non_resolue", 0)
    missing_stops = counts.get("arrets_reference_non_resolue", 0)
    if planned <= 0 or enriched != planned or missing_trips or missing_stops:
        raise ValueError(
            f"Compatibilité GTFS incomplète : courses={enriched}/{planned}, "
            f"courses_non_resolues={missing_trips}, arrets_non_resolus={missing_stops}"
        )
    return planned


def java_callbacks(project_root):
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
    if not java:
        raise ValueError("Java introuvable.")
    dependencies = (project_root / "producer-java/target/collector-classpath.txt").read_text(
        encoding="utf-8"
    ).strip()
    if not dependencies:
        raise ValueError("Classpath Java vide.")
    classpath = str(project_root / "producer-java/target/classes") + os.pathsep + dependencies
    prefix = [java, "-cp", classpath]

    def collect(source, root):
        run_java(prefix + [RAW_MAIN, source, str(root), "1", "1"], project_root)

    def export(snapshot, archive, output):
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as capture:
            try:
                run_java(prefix + [INSPECTOR, str(snapshot), str(archive)], project_root, capture)
            except BaseException:
                capture.seek(0)
                print(capture.read(), flush=True)
                raise
            capture.seek(0)
            text = capture.read()
        try:
            planned = check_reference_output(text)
        except (ValueError, KeyError):
            print(text, flush=True)
            raise
        print(f"status=reference_check planned={planned} enriched={planned}", flush=True)
        run_java(prefix + [EXPORTER, str(snapshot), str(archive), str(output)], project_root)

    return collect, export


def main():
    parser = argparse.ArgumentParser(description="Collecter et normaliser une session SNCF, sans Kafka.")
    parser.add_argument("source_url")
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("gtfs_archive", type=Path)
    parser.add_argument("reference_root", type=Path)
    parser.add_argument("--interval", type=int, default=120)
    parser.add_argument("--cycles", type=int, default=3)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    previous_handlers = {}

    def request_stop(signum, frame):
        raise StopRequested(signum)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, request_stop)
        collect, exporter = java_callbacks(project)
        result = run_session(
            args.source_url, args.collection_root, args.gtfs_archive,
            args.reference_root, args.interval, args.cycles,
            collect, exporter, reporter=lambda message: print(message, flush=True),
        )
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except StopRequested as stop:
        print(f"status=session_interrupted signal={signal.Signals(stop.signum).name}", file=sys.stderr, flush=True)
        return 128 + stop.signum
    except (Exception, KeyboardInterrupt) as exception:
        print(f"status=session_failed exception={type(exception).__name__} message={exception}", file=sys.stderr, flush=True)
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())