import argparse
import fcntl
import json
import signal
import sys
import tempfile
from pathlib import Path
from threading import Event

from real_active_profile import build_real_environment
from real_active_supervision import (
    build_real_job_commands,
    supervise_real_jobs,
)
from run_active_simulation import positive_integer
from real_session_config import (
    build_ingestion_command,
    load_ingestion_template,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def prepare_real_pipeline(
    project_root,
    reference_root,
    interval=30,
    gold_interval=60,
):
    project = Path(project_root).resolve()
    env = build_real_environment(
        project, reference_root, interval=interval
    )

    lakehouse = Path(env["LAKEHOUSE_ROOT"])
    checkpoints = Path(env["CHECKPOINT_ROOT"])

    required = (
        lakehouse / "bronze/trip_updates/_delta_log",
        lakehouse / "silver/trip_delays/_delta_log",
        lakehouse / "silver/rejected_events/_delta_log",
        checkpoints / "bronze_trip_updates",
        checkpoints / "silver_trip_delays",
        checkpoints / "silver_rejected_events",
        project / "data/source_checks",
    )
    for directory in required:
        if not directory.is_dir():
            raise ValueError(
                f"Dossier existant requis, sans réinitialisation : "
                f"{directory}"
            )

    commands = build_real_job_commands(
        project,
        env["GTFS_REFERENCE_ROOT"],
        gold_interval=gold_interval,
    )
    for _, command in commands:
        if not Path(command[1]).is_file():
            raise ValueError(f"Script absent : {command[1]}")

    return env, commands



def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Superviser Bronze, Silver et Gold/PostgreSQL réels, "
            "sans collecte ni publication Kafka."
        )
    )
    parser.add_argument("reference_root", type=Path)
    parser.add_argument("--interval", type=positive_integer, default=30)
    parser.add_argument(
        "--gold-interval", type=positive_integer, default=60
    )
    parser.add_argument(
        "--stop-timeout", type=positive_integer, default=60
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-only", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--ingestion-template", type=Path)
    parser.add_argument(
        "--collection-interval", type=positive_integer, default=120
    )
    parser.add_argument(
        "--collection-cycles", type=positive_integer, default=3
    )
    parser.add_argument(
        "--publication-limit", type=positive_integer, default=100
    )
    args = parser.parse_args(argv)

    previous_handlers = {}

    try:
        env, commands = prepare_real_pipeline(
            PROJECT_ROOT,
            args.reference_root,
            args.interval,
            args.gold_interval,
        )

        template = None

        if args.publication_limit > 20_000:
            raise ValueError("La limite de publication dépasse 20000.")

        if args.ingestion_template is not None:
            template = load_ingestion_template(
                PROJECT_ROOT,
                args.ingestion_template,
                env["GTFS_REFERENCE_ROOT"],
            )

        for name in (
            "KAFKA_TRIP_UPDATES_TOPIC",
            "LAKEHOUSE_ROOT",
            "CHECKPOINT_ROOT",
            "GTFS_REFERENCE_ROOT",
            "POSTGRES_TARGET_SCHEMA",
        ):
            print(f"{name}={env[name]}", flush=True)

        if args.check_only:
            print(
                "Précontrôles locaux réussis. Aucun job démarré.",
                flush=True,
            )
            return 0

        lock_path = PROJECT_ROOT / "data/.active-real-pipeline.lock"

        with lock_path.open("a", encoding="utf-8") as lock:
            fcntl.flock(
                lock.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )

            session = Path(tempfile.mkdtemp(
                prefix="active-real-processing-",
                dir=PROJECT_ROOT / "data/source_checks",
            ))
            logs = session / "logs"
            finite_jobs = ()

            if template is not None:
                collection = session / "collection"
                commands = commands + [(
                    "ingestion",
                    build_ingestion_command(
                        PROJECT_ROOT,
                        template,
                        collection,
                        interval=args.collection_interval,
                        cycles=args.collection_cycles,
                        limit=args.publication_limit,
                    ),
                )]
                finite_jobs = ("ingestion",)

                print(
                    f"Nouvelle collecte supervisée : {collection}",
                    flush=True,
                )
            stop_requested = Event()

            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.signal(
                    signum,
                    lambda signum, frame: stop_requested.set(),
                )

            print(f"Dossier de traitement : {session}", flush=True)
            print(f"Logs : {logs}", flush=True)
            print("Ctrl+C demande l'arrêt des trois jobs.", flush=True)

            code = supervise_real_jobs(
                project_root=PROJECT_ROOT,
                env=env,
                commands=commands,
                log_directory=logs,
                stop_requested=stop_requested,
                stop_timeout=args.stop_timeout,
                finite_jobs=finite_jobs,
            )

            with (session / "exit-code.json").open(
                "x", encoding="utf-8"
            ) as file:
                json.dump({"exit_code": code}, file, indent=2)

            return code

    except BlockingIOError:
        print(
            "Un autre superviseur réel détient déjà le verrou.",
            file=sys.stderr,
        )
        return 1

    except Exception as error:
        print(
            f"Erreur du lanceur réel : {error}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())