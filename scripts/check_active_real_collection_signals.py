import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


JAVA_CLASSES = (
    "fr.remyqueny.sncf.producer.GtfsRealtimeCollectorApplication",
    "fr.remyqueny.sncf.producer.GtfsRealtimeEnrichmentInspector",
    "fr.remyqueny.sncf.producer.GtfsRealtimeJsonExporter",
)


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def json_lines(text):
    result = []
    for line in text.splitlines():
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                result.append(value)
    return result


def owned_java_pids(collection_root):
    listing = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        check=True, capture_output=True, text=True,
    ).stdout
    result = []
    for line in listing.splitlines():
        parts = line.strip().split(maxsplit=1)
        if len(parts) != 2:
            continue
        pid, command = parts
        if str(collection_root) in command and any(name in command for name in JAVA_CLASSES):
            result.append(int(pid))
    return result


def cleanup_java(collection_root):
    initial = owned_java_pids(collection_root)
    for signum, timeout in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        for pid in owned_java_pids(collection_root):
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + timeout
        while owned_java_pids(collection_root) and time.monotonic() < deadline:
            time.sleep(0.1)
        if not owned_java_pids(collection_root):
            break
    if owned_java_pids(collection_root):
        raise RuntimeError("Un processus Java du contrôle n'a pas pu être arrêté.")
    return initial


def cleanup_process(process):
    if process is None or process.poll() is not None:
        return False
    for signum, timeout in ((signal.SIGTERM, 45), (signal.SIGKILL, 5)):
        if process.poll() is not None:
            break
        try:
            process.send_signal(signum)
        except ProcessLookupError:
            break
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            continue
    if process.poll() is None:
        raise RuntimeError("Le lanceur Python du contrôle n'a pas terminé.")
    return True


def run_case(mode, project, archive, references, body, expected_events, output_root):
    case = output_root / mode
    case.mkdir()
    collection = case / "collection"
    ready = threading.Event()
    release_response = threading.Event()
    state = {"mode": mode, "requests": 0}
    lock = threading.Lock()
    failures = []
    process = None
    reader = None
    interrupted_code = None
    resume_code = None
    shutdown_seconds = None
    forced = False
    baseline_manifest = None
    final_manifest = None
    captured_lines = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            with lock:
                current_mode = state["mode"]
                state["requests"] += 1
            if self.path != "/feed":
                self.send_error(404)
                return
            if current_mode == "http_in_flight":
                ready.set()
                release_response.wait(timeout=90)
            status = 503 if current_mode == "retry_backoff" else 200
            response_body = b"Service unavailable" if status == 503 else body
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", str(len(response_body)))
                if status == 503:
                    self.send_header("Retry-After", "120")
                self.end_headers()
                self.wfile.write(response_body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format_string, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.block_on_close = False
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    source = f"http://127.0.0.1:{server.server_address[1]}/feed"
    command = [
        sys.executable, str(project / "scripts/run_active_real_collection.py"),
        source, str(collection), str(archive), str(references),
        "--interval", "300", "--cycles",
    ]
    first_log = case / "interrupted.log"
    resume_log = case / "resumed.log"
    checks = {}

    try:
        with first_log.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                command + ["10"], cwd=project,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", start_new_session=True,
            )

            def read_output():
                for line in process.stdout:
                    captured_lines.append(line)
                    try:
                        log.write(line)
                        log.flush()
                    except ValueError:
                        pass
                    if mode == "retry_backoff" and "status=retry " in line:
                        ready.set()
                    if mode == "cycle_wait":
                        for value in json_lines(line):
                            if value.get("stage") == "cycle" and value.get("cycle") == 1:
                                ready.set()

            reader = threading.Thread(target=read_output, daemon=True)
            reader.start()
            deadline = time.monotonic() + 60
            while not ready.wait(0.1):
                if process.poll() is not None:
                    raise RuntimeError("Le lanceur a terminé avant le point d'interruption.")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Point d'interruption non atteint en 60 s.")
            if process.poll() is not None:
                raise RuntimeError("Le lanceur a terminé avant SIGINT.")
            started = time.monotonic()
            process.send_signal(signal.SIGINT)
            interrupted_code = process.wait(timeout=45)
            shutdown_seconds = round(time.monotonic() - started, 3)
            reader.join(timeout=5)
            if reader.is_alive():
                raise RuntimeError("La sortie du lanceur reste ouverte après son arrêt.")
            process.stdout.close()

        first_text = first_log.read_text(encoding="utf-8")
        checks["interrupted_exit_130"] = interrupted_code == 130
        checks["interruption_reported"] = "status=session_interrupted signal=SIGINT" in first_text
        checks["no_false_completion"] = not any(
            value.get("status") == "session_completed" for value in json_lines(first_text)
        )
        checks["no_session_failure"] = "status=session_failed" not in first_text
        checks["no_shutdown_timeout"] = "status=shutdown_timeout" not in first_text
        checks["no_orphan_java_after_stop"] = not owned_java_pids(collection)
        checks["no_partial_after_stop"] = not list(collection.rglob("*.partial"))
        expected_before = 1 if mode == "cycle_wait" else 0
        checks["archives_before_resume"] = len(list(collection.rglob("tripupdates.pb"))) == expected_before
        manifest_path = collection / "normalization_manifest.json"
        if manifest_path.exists():
            baseline_manifest = manifest_path.read_bytes()
        checks["manifest_before_resume"] = (baseline_manifest is not None) == bool(expected_before)

        if not all(checks.values()):
            raise RuntimeError("Échec du contrôle d'arrêt ; reprise non lancée.")

        release_response.set()
        with lock:
            state["mode"] = "success"
            requests_before_resume = state["requests"]
        process = None
        with resume_log.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                command + ["1"], cwd=project,
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            resume_code = process.wait(timeout=60)
        resume_text = resume_log.read_text(encoding="utf-8")
        summaries = [
            value for value in json_lines(resume_text)
            if value.get("status") == "session_completed"
        ]
        final_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        checks["resume_exit_zero"] = resume_code == 0
        checks["one_resume_request"] = state["requests"] - requests_before_resume == 1
        checks["resume_completed_once"] = len(summaries) == 1 and summaries[0].get("completed_cycles") == 1
        checks["kafka_disabled"] = len(summaries) == 1 and summaries[0].get("kafka_enabled") is False
        checks["expected_normalization_count"] = len(summaries) == 1 and summaries[0].get("newly_normalized") == (0 if mode == "cycle_wait" else 1)
        checks["one_indexed_snapshot"] = len(final_manifest["snapshots"]) == 1
        checks["expected_event_count"] = final_manifest["total_events"] == expected_events
        checks["one_archive"] = len(list(collection.rglob("tripupdates.pb"))) == 1
        checks["one_jsonl"] = len(list(collection.rglob("tripupdates.canonical.jsonl"))) == 1
        checks["no_partial_after_resume"] = not list(collection.rglob("*.partial"))
        checks["no_orphan_java_after_resume"] = not owned_java_pids(collection)
        if baseline_manifest is not None:
            checks["existing_manifest_unchanged"] = manifest_path.read_bytes() == baseline_manifest
            resumes = [value for value in json_lines(resume_text) if value.get("stage") == "resume"]
            checks["resume_skips_existing_export"] = (
                len(resumes) == 1
                and resumes[0].get("newly_normalized") == 0
                and resumes[0].get("already_normalized") == 1
            )
        for record in final_manifest["snapshots"]:
            checks["validated_export_hash"] = sha256(record["jsonl_path"]) == record["jsonl_sha256"]
    except Exception as exception:
        failures.append(f"{type(exception).__name__}: {exception}")
    finally:
        release_response.set()
        try:
            forced = cleanup_process(process)
        except Exception as exception:
            failures.append(f"Nettoyage lanceur : {exception}")
        try:
            orphans = cleanup_java(collection)
            if orphans:
                failures.append(f"JVM résiduelles nettoyées : {orphans}")
        except Exception as exception:
            failures.append(f"Nettoyage Java : {exception}")
        if reader is not None:
            reader.join(timeout=5)
            if reader.is_alive():
                failures.append("Le lecteur des logs n'a pas terminé après nettoyage.")
        first_log.write_text("".join(captured_lines), encoding="utf-8")
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    if forced:
        failures.append("Le processus Python a nécessité un arrêt de nettoyage.")
    failures.extend(name for name, passed in checks.items() if not passed)
    return {
        "scenario": mode, "passed": not failures,
        "interrupted_exit_code": interrupted_code, "resume_exit_code": resume_code,
        "shutdown_seconds": shutdown_seconds,
        "indexed_events": final_manifest["total_events"] if final_manifest else None,
        "checks": checks, "failures": failures,
        "interrupted_log": str(first_log), "resume_log": str(resume_log),
    }


def main():
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Tester SIGINT et reprise du lanceur réel, sans accès SNCF ni Kafka.")
    parser.add_argument("fixture_collection", type=Path)
    parser.add_argument("gtfs_archive", type=Path)
    parser.add_argument("reference_root", type=Path)
    parser.add_argument("--output-root", type=Path, default=project / "data/validation")
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("macOS ou Linux requis.")
    manifest = json.loads((args.fixture_collection / "normalization_manifest.json").read_text(encoding="utf-8"))
    if not manifest["snapshots"]:
        parser.error("Aucun instantané normalisé disponible comme fixture.")
    record = max(manifest["snapshots"], key=lambda item: item["published_at"])
    archive = args.gtfs_archive.resolve()
    references = args.reference_root.resolve()
    if sha256(archive) != manifest["gtfs_archive_sha256"]:
        parser.error("L'archive ne correspond pas au manifeste de la fixture.")
    if str(references) != manifest["reference_root"]:
        parser.error("Le dossier de références diffère de celui de la fixture.")
    if any(sha256(references / name) != value for name, value in manifest["reference_files_sha256"].items()):
        parser.error("Les références de la fixture ont changé.")
    snapshot = Path(record["snapshot_path"])
    if sha256(snapshot) != record["snapshot_sha256"]:
        parser.error("L'instantané utilisé comme fixture a changé.")
    body = snapshot.read_bytes()
    args.output_root.mkdir(parents=True, exist_ok=True)
    output_root = Path(tempfile.mkdtemp(
        prefix="collector-signals-active-", dir=args.output_root.resolve()
    ))
    print(f"Dossier de preuves : {output_root}", flush=True)
    results = []
    for mode in ("cycle_wait", "retry_backoff", "http_in_flight"):
        result = run_case(mode, project, archive, references, body, record["event_count"], output_root)
        results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    report = {"all_passed": all(item["passed"] for item in results), "cases": results}
    (output_root / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Bilan : {sum(item['passed'] for item in results)}/3 scénarios arrêt/reprise réussis.")
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())