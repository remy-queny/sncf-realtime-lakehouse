import os
from datetime import datetime
from pathlib import Path

import psycopg
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format


PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLD_ROOT = PROJECT_ROOT / "data" / "lakehouse" / "gold"
MAX_ROWS_PER_TABLE = 10_000


LINE_UPSERT = """
INSERT INTO gold_delay_by_line_15min (
    window_start, window_end, route_id, line_name,
    event_count, delayed_trip_count,
    average_delay_minutes, max_delay_minutes
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, route_id) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    line_name = EXCLUDED.line_name,
    event_count = EXCLUDED.event_count,
    delayed_trip_count = EXCLUDED.delayed_trip_count,
    average_delay_minutes = EXCLUDED.average_delay_minutes,
    max_delay_minutes = EXCLUDED.max_delay_minutes
"""


STATION_UPSERT = """
INSERT INTO gold_station_delay_daily (
    event_date, stop_id, stop_name, delayed_event_count,
    average_delay_minutes, max_delay_minutes, event_count
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_date, stop_id) DO UPDATE SET
    stop_name = EXCLUDED.stop_name,
    delayed_event_count = EXCLUDED.delayed_event_count,
    average_delay_minutes = EXCLUDED.average_delay_minutes,
    max_delay_minutes = EXCLUDED.max_delay_minutes,
    event_count = EXCLUDED.event_count
"""


def collect_small_table(dataframe, table_name):
    rows = dataframe.limit(MAX_ROWS_PER_TABLE + 1).collect()
    if len(rows) > MAX_ROWS_PER_TABLE:
        raise ValueError(
            f"{table_name} dépasse {MAX_ROWS_PER_TABLE} lignes : "
            "arrêt pour éviter de tout charger sur le Mac."
        )
    return rows


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("sncf-publish-gold-postgres")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        lines = (
            spark.read.format("delta")
            .load(str(GOLD_ROOT / "delay_by_line_15min"))
            .select(
                date_format("window_start", "yyyy-MM-dd HH:mm:ss")
                .alias("window_start_utc"),
                date_format("window_end", "yyyy-MM-dd HH:mm:ss")
                .alias("window_end_utc"),
                "route_id",
                "line_name",
                "event_count",
                "delayed_trip_count",
                "average_delay_minutes",
                "max_delay_minutes",
            )
        )
        stations = (
            spark.read.format("delta")
            .load(str(GOLD_ROOT / "station_delay_daily"))
        )

        line_rows = [
            (
                datetime.fromisoformat(row.window_start_utc),
                datetime.fromisoformat(row.window_end_utc),
                row.route_id,
                row.line_name,
                row.event_count,
                row.delayed_trip_count,
                row.average_delay_minutes,
                row.max_delay_minutes,
            )
            for row in collect_small_table(lines, "Gold par ligne")
        ]
        station_rows = [
            (
                row.event_date,
                row.stop_id,
                row.stop_name,
                row.delayed_event_count,
                row.average_delay_minutes,
                row.max_delay_minutes,
                row.event_count,
            )
            for row in collect_small_table(
                stations, "Gold par gare"
            )
        ]

        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(LINE_UPSERT, line_rows)
                cursor.executemany(STATION_UPSERT, station_rows)

        print(f"Gold line rows published: {len(line_rows)}")
        print(f"Gold station rows published: {len(station_rows)}")
        print("Gold-to-PostgreSQL publication completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()