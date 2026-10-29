import argparse
import fcntl
import json
import os
import signal
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from threading import Event

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "spark/src"))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from run_active_simulation import (
    build_environment,
    positive_integer,
    validate_test_directory,
)
from jobs.gold_cycle import get_latest_silver_version, run_gold_cycle
from jobs.postgres_target import validate_postgres_target
from jobs.stream_silver import create_spark_session


def load_postgres_environment():
    for variable in ("POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PASSWORD"):
        value = subprocess.check_output(
            [
                "docker", "compose", "-p", "sncf-realtime-lakehouse",
                "exec", "-T", "postgres", "sh", "-c",
                f'printf "%s" "${variable}"',
            ],
            cwd=PROJECT_ROOT,
            text=True,
            timeout=15,
        )
        if not value:
            raise RuntimeError(f"Variable absente : {variable}")
        os.environ[variable] = value


def refresh_if_needed(
    spark,
    lakehouse_root,
    last_published_version,
    *,
    target_schema="sncf_active_test",
):
    validate_postgres_target(
        lakehouse_root,
        target_schema,
        PROJECT_ROOT,
    )

    version = get_latest_silver_version(
        spark,
        lakehouse_root / "silver/trip_delays",
    )

    if version == last_published_version:
        return None

    return run_gold_cycle(
        spark=spark,
        lakehouse_root=lakehouse_root,
        target_schema=target_schema,
        silver_version=version,
    )


def run_refresh_loop(
    spark,
    lakehouse_root,
    interval,
    stop_requested,
    once=False,
    *,
    target_schema="sncf_active_test",
):
    last_published_version = None

    while not stop_requested.is_set():
        result = refresh_if_needed(
            spark,
            lakehouse_root,
            last_published_version,
            target_schema=target_schema,
        )

        if result is None:
            print(
                f"Silver inchangée : version {last_published_version}, "
                "cycle ignoré.",
                flush=True,
            )
        else:
            last_published_version = result.silver_version
            print(
                json.dumps(asdict(result), ensure_ascii=False),
                flush=True,
            )
            print(
                f"Cycle Gold/PostgreSQL terminé : {target_schema}.",
                flush=True,
            )

        if once:
            break

        stop_requested.wait(interval)

def create_interruptible_spark_session(stop_requested):
    spark = create_spark_session()

    def request_stop(signum, frame):
        stop_requested.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)

    return spark

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Actualiser périodiquement Gold/PostgreSQL sur un périmètre de test existant."
    )
    parser.add_argument("test_directory", type=Path)
    parser.add_argument("--interval", type=positive_integer, default=60)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    try:
        directory = validate_test_directory(PROJECT_ROOT, args.test_directory)
        lakehouse_root = directory / "lakehouse"
        validate_postgres_target(lakehouse_root, "sncf_active_test", PROJECT_ROOT)
    except ValueError as error:
        parser.error(str(error))

    lock_path = PROJECT_ROOT / "data/validation/.active-gold.lock"
    stop_requested = Event()
    spark = None
    previous_handlers = {}
    exit_code = 0

    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Un autre lanceur Gold détient déjà le verrou.", file=sys.stderr)
            return 1
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.signal(
                    signum, lambda signum, frame: stop_requested.set()
                )
            os.environ.update(build_environment(PROJECT_ROOT, directory, 10))
            os.environ.update({
                "POSTGRES_TARGET_SCHEMA": "sncf_active_test",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
            })
            load_postgres_environment()
            print(f"Lakehouse : {lakehouse_root}", flush=True)
            print("Cible PostgreSQL : sncf_active_test", flush=True)
            print(f"Pause entre contrôles : {args.interval} secondes", flush=True)
            print("Ctrl+C demande un arrêt après le cycle en cours.", flush=True)
            spark = create_interruptible_spark_session(
                stop_requested
            )
            spark.sparkContext.setLogLevel("WARN")
            spark.conf.set("spark.sql.shuffle.partitions", "2")
            run_refresh_loop(
                spark=spark,
                lakehouse_root=lakehouse_root,
                interval=args.interval,
                stop_requested=stop_requested,
                once=args.once,
            )
        except Exception as error:
            print(f"Erreur du lanceur Gold : {error}", file=sys.stderr, flush=True)
            exit_code = 1
        finally:
            try:
                if spark is not None:
                    spark.stop()
            finally:
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)
    print(f"Lanceur Gold terminé, code {exit_code}.", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
