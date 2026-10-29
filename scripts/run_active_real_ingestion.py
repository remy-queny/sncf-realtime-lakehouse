import argparse
import json
import signal
import sys
from pathlib import Path

from publish_real_publication import java_prefix
from real_publication_cycle import run_publishing_session
from run_active_real_collection import StopRequested, java_callbacks

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def positive_integer(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError(
            "La valeur doit être strictement positive."
        )
    return number


def validate_java(project):
    java_prefix(project)

    package = (
        project
        / "producer-java/target/classes/fr/remyqueny/sncf/producer"
    )

    for name in (
        "GtfsRealtimeCollectorApplication",
        "GtfsRealtimeEnrichmentInspector",
        "GtfsRealtimeJsonExporter",
    ):
        if not (package / f"{name}.class").is_file():
            raise ValueError(f"Classe Java non compilée : {name}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Collecter, normaliser et publier une nouvelle session SNCF "
            "dans Kafka. Aucun traitement Spark."
        )
    )
    parser.add_argument("source_url")
    parser.add_argument("collection_root", type=Path)
    parser.add_argument("gtfs_archive", type=Path)
    parser.add_argument("reference_root", type=Path)
    parser.add_argument("--interval", type=positive_integer, default=120)
    parser.add_argument("--cycles", type=positive_integer, default=3)
    parser.add_argument("--limit", type=positive_integer, default=100)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)

    if not args.execute:
        parser.error("--execute est obligatoire pour autoriser les envois.")

    if args.limit > 20_000:
        parser.error("--limit ne doit pas dépasser 20000.")

    previous_handlers = {}

    def request_stop(signum, frame):
        raise StopRequested(signum)

    try:
        if args.collection_root.exists():
            raise ValueError("Un nouveau dossier inexistant est requis.")

        validate_java(PROJECT_ROOT)
        collect, exporter = java_callbacks(PROJECT_ROOT)

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(
                signum, request_stop
            )

        result = run_publishing_session(
            args.source_url,
            args.collection_root,
            args.gtfs_archive,
            args.reference_root,
            project_root=PROJECT_ROOT,
            collect=collect,
            exporter=exporter,
            interval=args.interval,
            cycles=args.cycles,
            limit=args.limit,
            bootstrap_servers=args.bootstrap_servers,
            reporter=lambda message: print(message, flush=True),
        )

        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0

    except StopRequested as stop:
        print(
            f"status=ingestion_interrupted "
            f"signal={signal.Signals(stop.signum).name}",
            file=sys.stderr,
            flush=True,
        )
        return 128 + stop.signum

    except Exception as error:
        print(
            f"status=ingestion_failed "
            f"exception={type(error).__name__} message={error}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())