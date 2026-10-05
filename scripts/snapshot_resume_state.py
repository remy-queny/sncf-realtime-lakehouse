import json
import subprocess
import sys
from pathlib import Path

from jobs.stream_silver import create_spark_session

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = PROJECT_ROOT / "data" / "lakehouse" / "silver"

POSTGRES_SQL = """
SELECT 'gold_delay_by_line_15min';
SELECT row_to_json(t)::text
FROM (
    SELECT window_start, window_end, route_id, line_name,
           event_count, delayed_trip_count,
           average_delay_minutes, max_delay_minutes
    FROM gold_delay_by_line_15min
    ORDER BY window_start, route_id
) AS t;

SELECT 'gold_station_delay_daily';
SELECT row_to_json(t)::text
FROM (
    SELECT event_date, stop_id, stop_name, delayed_event_count,
           average_delay_minutes, max_delay_minutes, event_count
    FROM gold_station_delay_daily
    ORDER BY event_date, stop_id
) AS t;
"""

POSTGRES_COMMAND = """
export PGPASSWORD="${POSTGRES_PASSWORD:-}"
export PGOPTIONS="-c timezone=UTC"
exec psql -X -q -A -t -v ON_ERROR_STOP=1 \
    -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" \
    -c "$1"
"""


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage : snapshot_resume_state.py <fichier.json>"
        )

    output = Path(sys.argv[1])
    if output.exists():
        raise SystemExit(f"Le fichier existe déjà : {output}")

    state = {}
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in ("trip_delays", "rejected_events"):
            rows = (
                spark.read.format("delta")
                .load(str(SILVER_ROOT / table))
                .limit(10_001)
                .toJSON()
                .collect()
            )

            if len(rows) > 10_000:
                raise RuntimeError(
                    f"{table} dépasse la limite du contrôle local."
                )

            state[table] = sorted(rows)
            print(f"{table} : {len(rows)} lignes")
    finally:
        spark.stop()

    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "sh", "-c", POSTGRES_COMMAND,
            "snapshot", POSTGRES_SQL,
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    state["postgres"] = result.stdout

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)

    print(f"État enregistré : {output}")


if __name__ == "__main__":
    main()