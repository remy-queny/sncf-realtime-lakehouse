import argparse
import fcntl
import os
import signal
import subprocess
import sys
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def positive_integer(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("La valeur doit être strictement positive.")
    return number


def validate_test_directory(project_root, directory):
    root = project_root.resolve()
    path = Path(directory)
    if not path.is_absolute():
        path = root / path
    path = path.resolve()
    validation = (root / "data/validation").resolve()
    if path == validation or not path.is_relative_to(validation):
        raise ValueError("Le dossier doit être un sous-dossier de data/validation.")
    required = (
        path / "lakehouse/bronze/trip_updates/_delta_log",
        path / "checkpoints/bronze",
        path / "checkpoints/silver_trip_delays",
        path / "checkpoints/silver_rejected_events",
    )
    for directory in required:
        if not directory.is_dir():
            raise ValueError(f"Dossier de test existant requis : {directory}")
    for filename in ("routes.csv", "stops.csv"):
        reference = root / "spark/resources/simulation" / filename
        if not reference.is_file():
            raise ValueError(f"Référence absente : {reference}")
    return path


def build_environment(project_root, directory, interval, base=None):
    env = dict(os.environ if base is None else base)
    env.update({
        "PYTHONPATH": str(project_root / "spark/src"),
        "PYSPARK_PYTHON": sys.executable,
        "PYTHONUNBUFFERED": "1",
        "SPARK_MASTER": "local[2]",
        "STREAMING_MODE": "active",
        "STREAMING_INTERVAL_SECONDS": str(interval),
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_TRIP_UPDATES_TOPIC": "sncf.trip_updates.raw",
        "LAKEHOUSE_ROOT": str(directory / "lakehouse"),
        "CHECKPOINT_ROOT": str(directory / "checkpoints"),
        "GTFS_REFERENCE_ROOT": str(project_root / "spark/resources/simulation"),
        "BRONZE_TRIP_UPDATES_PATH": str(directory / "lakehouse/bronze/trip_updates"),
        "BRONZE_TRIP_UPDATES_CHECKPOINT_PATH": str(directory / "checkpoints/bronze"),
    })
    env.pop("POSTGRES_TARGET_SCHEMA", None)
    return env


def build_job_commands(project_root, directory, with_gold=False, gold_interval=30):
    commands = [
        (name, [sys.executable, str(project_root / f"spark/src/jobs/stream_{name}.py")])
        for name in ("bronze", "silver")
    ]
    if with_gold:
        commands.append((
            "gold",
            [
                sys.executable, str(project_root / "scripts/run_active_gold.py"),
                str(directory), "--interval", str(gold_interval),
            ],
        ))
    return commands


def unexpected_exit(processes):
    for name, process in processes:
        code = process.poll()
        if code is not None:
            return name, code
    return None


def signal_group(process, signum):
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


def stop_process(name, process, timeout):
    if process.poll() is not None:
        return False
    forced = False
    for signum, wait_seconds in (
        (signal.SIGINT, timeout),
        (signal.SIGTERM, 10),
        (signal.SIGKILL, 10),
    ):
        print(f"Arrêt {name} : {signal.Signals(signum).name}", flush=True)
        if name == "gold" and signum == signal.SIGINT:
            process.send_signal(signum)
        else:
            signal_group(process, signum)
        try:
            code = process.wait(timeout=wait_seconds)
            if name == "gold" and code != 0:
                raise RuntimeError(f"Gold s'est arrêté avec le code {code}.")
            return forced
        except subprocess.TimeoutExpired:
            forced = True
    raise RuntimeError(f"Impossible de confirmer l'arrêt de {name}.")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Superviser Bronze/Silver et, en option, Gold/PostgreSQL en simulation."
    )
    parser.add_argument("test_directory", type=Path)
    parser.add_argument("--interval", type=positive_integer, default=10)
    parser.add_argument("--stop-timeout", type=positive_integer, default=30)
    parser.add_argument("--with-gold", action="store_true")
    parser.add_argument("--gold-interval", type=positive_integer, default=30)
    args = parser.parse_args(argv)
    try:
        directory = validate_test_directory(PROJECT_ROOT, args.test_directory)
        if args.with_gold and not (PROJECT_ROOT / "scripts/run_active_gold.py").is_file():
            raise ValueError("Le fichier scripts/run_active_gold.py est absent.")
    except ValueError as error:
        parser.error(str(error))

    stop_requested = Event()
    previous_handlers = {}
    processes = []
    exit_code = 0
    log_directory = directory / "logs" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    )
    lock_path = PROJECT_ROOT / "data/validation/.active-simulation.lock"

    with ExitStack() as stack:
        lock = stack.enter_context(lock_path.open("a+"))
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Un autre lanceur de simulation détient déjà le verrou.", file=sys.stderr)
            return 1
        log_directory.mkdir(parents=True, exist_ok=False)
        env = build_environment(PROJECT_ROOT, directory, args.interval)
        commands = build_job_commands(
            PROJECT_ROOT, directory, args.with_gold, args.gold_interval
        )
        print(f"Simulation uniquement : {directory}", flush=True)
        print(f"Logs : {log_directory}", flush=True)
        if args.with_gold:
            print("Gold activé : publication uniquement dans sncf_active_test.", flush=True)
        else:
            print("Gold et publication PostgreSQL désactivés.", flush=True)
        print("Aucune production Kafka effectuée par le lanceur.", flush=True)
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.signal(
                    signum, lambda signum, frame: stop_requested.set()
                )
            for name, command in commands:
                if stop_requested.is_set():
                    break
                log = stack.enter_context((log_directory / f"{name}.log").open("w"))
                process = subprocess.Popen(
                    command, cwd=PROJECT_ROOT, env=env, stdout=log,
                    stderr=subprocess.STDOUT, start_new_session=True,
                )
                processes.append((name, process))
                print(f"Processus {name} lancé : PID {process.pid}", flush=True)
            print("Surveillance des processus ; Ctrl+C pour demander leur arrêt.", flush=True)
            print("Un PID vivant ne prouve pas la progression des micro-batches.", flush=True)
            while not stop_requested.wait(0.5):
                failure = unexpected_exit(processes)
                if failure is not None:
                    name, code = failure
                    raise RuntimeError(f"Sortie inattendue de {name}, code {code}. Voir ses logs.")
        except Exception as error:
            print(f"Erreur : {error}", file=sys.stderr, flush=True)
            exit_code = 1
        finally:
            for name, process in reversed(processes):
                try:
                    if stop_process(name, process, args.stop_timeout):
                        exit_code = 1
                except Exception as error:
                    print(f"Erreur pendant l'arrêt de {name} : {error}", file=sys.stderr)
                    exit_code = 1
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
    print(f"Lanceur terminé, code {exit_code}.", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
