from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    when,
    window,
)


def aggregate_delay_by_line(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy(
            window(col("event_timestamp"), "15 minutes"),
            col("route_id"),
            col("line_name"),
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct(
                when(col("delay_minutes") > 5, col("trip_id"))
            ).alias("delayed_trip_count"),
            spark_round(
                avg("delay_minutes"), 2
            ).alias("average_delay_minutes"),
            spark_max("delay_minutes").alias("max_delay_minutes"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "route_id",
            "line_name",
            "event_count",
            "delayed_trip_count",
            "average_delay_minutes",
            "max_delay_minutes",
        )
    )


def aggregate_station_delay_daily(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy("event_date", "stop_id", "stop_name")
        .agg(
            spark_sum(
                when(col("delay_minutes") > 5, 1).otherwise(0)
            ).alias("delayed_event_count"),
            spark_round(
                avg("delay_minutes"), 2
            ).alias("average_delay_minutes"),
            spark_max("delay_minutes").alias("max_delay_minutes"),
            count("*").alias("event_count"),
        )
    )