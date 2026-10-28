import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


MAIN_CLASS = "fr.remyqueny.sncf.producer.GtfsRealtimeCollectorApplication"


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def simulated_feed():
    timestamp = int(datetime(2026, 10, 28, 10, tzinfo=timezone.utc).timestamp())
    header = b"\x0a\x03" + b"1.0" + b"\x18" + varint(timestamp)
    return b"\x0a" + varint(len(header)) + header


def stop_owned_process(process):
    if process is None or process.poll() is not None:
        return False
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    return True


def run_case(mode, java, classpath, output_directory):
    directory = output_directory / mode
    directory.mkdir()
    collection_root = directory / "collection"
    log_path = directory / "collector.log"
    ready = threading.Event()
    release_response = threading.Event()
    request_lock = threading.Lock()
    requests = [0]
    failures = []
    process = None
    reader = None
    forced = False
    exit_code = None
    elapsed = None

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            with request_lock:
                requests[0] += 1

            if self.path != "/feed":
                self.send_error(404)
                return

            if mode == "http_in_flight":
                ready.set()
                release_response.wait(timeout=45)

            if mode == "retry_backoff":
                status = 503
                body = b"Service unavailable"
            else:
                status = 200
                body = simulated_feed()

            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", str(len(body)))
                if mode == "retry_backoff":
                    self.send_header("Retry-After", "120")
                self.end_headers()
                self.wfile.write(body)
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

    with log_path.open("w", encoding="utf-8") as log:
        try:
            process = subprocess.Popen(
                [
                    java, "-cp", classpath, MAIN_CLASS,
                    source, str(collection_root), "300", "10",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                start_new_session=True,
            )

            def read_output():
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    if mode == "cycle_wait" and "status=collected" in line:
                        ready.set()
                    if mode == "retry_backoff" and "status=retry " in line:
                        ready.set()

            reader = threading.Thread(target=read_output, daemon=True)
            reader.start()
            deadline = time.monotonic() + 30

            while not ready.wait(0.1):
                if process.poll() is not None:
                    raise RuntimeError("La JVM a terminé avant le point de test.")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Le point de test n'a pas été atteint en 30 s.")

            if process.poll() is not None:
                raise RuntimeError("La JVM a terminé avant l'envoi de SIGINT.")

            start = time.monotonic()
            process.send_signal(signal.SIGINT)
            exit_code = process.wait(timeout=20)
            elapsed = round(time.monotonic() - start, 3)
        except Exception as exception:
            failures.append(f"{type(exception).__name__}: {exception}")
        finally:
            release_response.set()
            forced = stop_owned_process(process)
            if reader is not None:
                reader.join(timeout=5)
                if reader.is_alive():
                    failures.append("Le lecteur des logs n'a pas terminé.")
            if process is not None:
                exit_code = process.returncode
                if process.stdout is not None and (reader is None or not reader.is_alive()):
                    process.stdout.close()
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=5)

    logs = log_path.read_text(encoding="utf-8")
    expected_archives = 1 if mode == "cycle_wait" else 0
    archives = list((collection_root / "snapshots").rglob("tripupdates.pb"))
    entries = list((collection_root / "registry").glob("*.json"))
    partials = list(collection_root.rglob("*.partial"))

    checks = {
        "no_forced_stop": not forced,
        "signal_exit_code": exit_code in (130, -signal.SIGINT),
        "shutdown_requested": "status=shutdown_requested" in logs,
        "interruption_reported": "status=interrupted" in logs,
        "shutdown_finished": "status=shutdown_finished" in logs,
        "no_shutdown_timeout": "status=shutdown_timeout" not in logs,
        "no_shutdown_wait_interruption": "status=shutdown_wait_interrupted" not in logs,
        "no_false_completion": "status=completed" not in logs,
        "no_collection_failure": "status=failed" not in logs,
        "one_http_request": requests[0] == 1,
        "expected_archives": len(archives) == expected_archives,
        "expected_registry_entries": len(entries) == expected_archives,
        "no_partial_files": not partials,
    }

    failures.extend(name for name, passed in checks.items() if not passed)

    return {
        "scenario": mode,
        "passed": not failures,
        "exit_code": exit_code,
        "shutdown_seconds": elapsed,
        "http_requests": requests[0],
        "archives": len(archives),
        "registry_entries": len(entries),
        "checks": checks,
        "failures": failures,
        "log": str(log_path),
    }


def main():
    root = Path(__file__).resolve().parents[1]
    default_java = None
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.is_file():
            default_java = str(candidate)
    default_java = default_java or shutil.which("java")

    parser = argparse.ArgumentParser(
        description="Vérifier SIGINT sur le collecteur Java avec un serveur HTTP local."
    )
    parser.add_argument("--java", default=default_java)
    parser.add_argument(
        "--classpath-file", type=Path,
        default=root / "producer-java/target/collector-classpath.txt",
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=root / "data/validation",
    )
    args = parser.parse_args()

    if os.name != "posix":
        parser.error("Ce contrôle nécessite macOS ou Linux.")
    if not args.java:
        parser.error("Java introuvable ; renseigner JAVA_HOME ou --java.")

    classes = root / "producer-java/target/classes"
    main_class_file = classes / (MAIN_CLASS.replace(".", "/") + ".class")
    if not main_class_file.is_file():
        parser.error("Classes Java absentes ; exécuter la compilation Maven indiquée.")

    classpath_file = args.classpath_file.resolve()
    if not classpath_file.is_file():
        parser.error("Classpath absent ; exécuter dependency:build-classpath.")
    dependencies = classpath_file.read_text(encoding="utf-8").strip()
    if not dependencies:
        parser.error("Le fichier de classpath est vide.")

    classpath = str(classes) + os.pathsep + dependencies
    args.output_root.mkdir(parents=True, exist_ok=True)
    output_directory = Path(tempfile.mkdtemp(
        prefix="collector-signals-", dir=args.output_root.resolve()
    ))
    print(f"Dossier de preuves : {output_directory}", flush=True)
    results = []

    for mode in ("cycle_wait", "retry_backoff", "http_in_flight"):
        result = run_case(mode, args.java, classpath, output_directory)
        results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)

    report = {
        "all_passed": all(result["passed"] for result in results),
        "java": args.java,
        "cases": results,
    }
    (output_directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Bilan : {sum(result['passed'] for result in results)}/3 scénarios réussis.")
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)