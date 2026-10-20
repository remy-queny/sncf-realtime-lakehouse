import argparse
import json
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAX_ROWS = 10_000
PROFILES = {
    "simulation": ("lakehouse", "public"),
    "real": ("lakehouse_real", "sncf_real"),
}

POSTGRES_SQL = """
SELECT 'gold_delay_by_line_15min';
SELECT row_to_json(t)::text
FROM (
    SELECT window_start, window_end, route_id, line_name,
           event_count, delayed_trip_count,
           average_delay_minutes, max_delay_minutes
    FROM {schema}.gold_delay_by_line_15min
    ORDER BY window_start, route_id
) AS t;

SELECT 'gold_station_delay_daily';
SELECT row_to_json(t)::text
FROM (
    SELECT event_date, stop_id, stop_name, delayed_event_count,
           average_delay_minutes, max_delay_minutes, event_count
    FROM {schema}.gold_station_delay_daily
    ORDER BY event_date, stop_id
) AS t;
"""

POSTGRES_COMMAND = """
export PGPASSWORD="${POSTGRES_PASSWORD:-}"
export PGOPTIONS="-c timezone=UTC -c default_transaction_read_only=on"
exec psql -X -q -A -t -v ON_ERROR_STOP=1 \
    -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" \
    -c "$1"
"""


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Capturer Silver et PostgreSQL sans lancer le pipeline."
    )
    parser.add_argument("output", type=Path, help="Nouveau fichier JSON de capture")
    parser.add_argument(
        "--mode", choices=tuple(PROFILES), default="simulation",
        help="Périmètre à capturer (défaut : simulation)",
    )
    return parser.parse_args(argv)


def postgres_counts(text):
    tables = ("gold_delay_by_line_15min", "gold_station_delay_daily")
    counts = {}
    current = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line in tables:
            if line in counts:
                raise RuntimeError("En-tête PostgreSQL dupliqué")
            current = line
            counts[current] = 0
        else:
            if current is None or not isinstance(json.loads(line), dict):
                raise RuntimeError("Sortie PostgreSQL inattendue")
            counts[current] += 1
    if set(counts) != set(tables):
        raise RuntimeError("Capture PostgreSQL incomplète")
    return counts


def main(argv=None):
    args = parse_args(argv)
    output = args.output
    if output.exists():
        raise SystemExit(f"Le fichier existe déjà : {output}")

    lakehouse, schema = PROFILES[args.mode]
    silver_root = PROJECT_ROOT / "data" / lakehouse / "silver"
    print(f"Mode : {args.mode}")
    print(f"Silver : {silver_root}")
    print(f"Schéma PostgreSQL : {schema}")

    from jobs.stream_silver import create_spark_session

    state = {}
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        for table in ("trip_delays", "rejected_events"):
            rows = (
                spark.read.format("delta")
                .load(str(silver_root / table))
                .limit(MAX_ROWS + 1)
                .toJSON()
                .collect()
            )
            if len(rows) > MAX_ROWS:
                raise RuntimeError(
                    f"{table} dépasse la limite du contrôle local ({MAX_ROWS})."
                )
            state[table] = sorted(rows)
            print(f"{table} : {len(rows)} lignes")
    finally:
        spark.stop()

    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "sh", "-c", POSTGRES_COMMAND,
            "snapshot", POSTGRES_SQL.format(schema=schema),
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    counts = postgres_counts(result.stdout)
    state["postgres"] = result.stdout
    for table, count in counts.items():
        print(f"{schema}.{table} : {count} lignes")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)
    print(f"État enregistré : {output}")


if __name__ == "__main__":
    main()