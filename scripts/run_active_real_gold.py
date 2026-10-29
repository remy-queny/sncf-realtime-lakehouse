import argparse
import fcntl
import os
import signal
import sys
from pathlib import Path
from threading import Event

from real_active_profile import build_real_environment
from run_active_gold import (
    create_interruptible_spark_session,
    load_postgres_environment,
    run_refresh_loop,
)
from run_active_simulation import positive_integer

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def prepare_real_gold(project_root, reference_root):
    project = Path(project_root).resolve()
    env = build_real_environment(project, reference_root)

    lakehouse = Path(env["LAKEHOUSE_ROOT"])
    checkpoints = Path(env["CHECKPOINT_ROOT"])

    required = (
        lakehouse / "silver/trip_delays/_delta_log",
        checkpoints,
    )
    for directory in required:
        if not directory.is_dir():
            raise ValueError(f"Dossier réel existant requis : {directory}")

    lock_path = project / "data/.active-real-gold.lock"
    return env, lakehouse, lock_path


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Actualiser Gold/PostgreSQL sur le lakehouse réel."
    )
    parser.add_argument("reference_root", type=Path)
    parser.add_argument(
        "--interval", type=positive_integer, default=60
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    try:
        env, lakehouse, lock_path = prepare_real_gold(
            PROJECT_ROOT, args.reference_root
        )
    except (OSError, ValueError) as error:
        print(f"Précontrôle Gold réel : {error}", file=sys.stderr)
        return 1

    stop_requested = Event()
    previous_handlers = {}
    spark = None
    exit_code = 0

    with lock_path.open("a", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                "Un autre lanceur Gold réel détient déjà le verrou.",
                file=sys.stderr,
            )
            return 1

        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.signal(
                    signum,
                    lambda signum, frame: stop_requested.set(),
                )

            os.environ.update(env)
            load_postgres_environment()

            print(f"Lakehouse réel : {lakehouse}", flush=True)
            print("Cible PostgreSQL : sncf_real", flush=True)
            print(
                "Ctrl+C demande un arrêt après le cycle en cours.",
                flush=True,
            )

            spark = create_interruptible_spark_session(stop_requested)
            spark.sparkContext.setLogLevel("WARN")
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.conf.set("spark.sql.shuffle.partitions", "2")

            run_refresh_loop(
                spark=spark,
                lakehouse_root=lakehouse,
                interval=args.interval,
                stop_requested=stop_requested,
                once=args.once,
                target_schema="sncf_real",
            )

        except Exception as error:
            print(
                f"Erreur Gold réel : {error}",
                file=sys.stderr,
                flush=True,
            )
            exit_code = 1

        finally:
            try:
                if spark is not None:
                    spark.stop()
            except Exception as error:
                print(
                    f"Erreur pendant l'arrêt Spark : {error}",
                    file=sys.stderr,
                    flush=True,
                )
                exit_code = 1
            finally:
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)

    print(f"Gold réel terminé, code {exit_code}.", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())