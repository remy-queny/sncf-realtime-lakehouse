from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, length, lit, to_timestamp, trim, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


TRIP_UPDATE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("scheduled_timestamp", StringType(), True),
        StructField("estimated_timestamp", StringType(), True),
        StructField("delay_seconds", IntegerType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def parse_and_validate_trip_updates(bronze_dataframe: DataFrame) -> DataFrame:
    parsed_dataframe = bronze_dataframe.withColumn(
        "_event",
        from_json(
            col("payload_json"),
            TRIP_UPDATE_SCHEMA,
            {"columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )

    typed_dataframe = parsed_dataframe.select(
        "*",
        col("_event.event_id").alias("event_id"),
        col("_event.event_type").alias("event_type"),
        col("_event.source").alias("source"),
        col("_event.schema_version").alias("schema_version"),
        col("_event.trip_id").alias("trip_id"),
        col("_event.route_id").alias("route_id"),
        col("_event.stop_id").alias("stop_id"),
        to_timestamp(
            col("_event.scheduled_timestamp")
        ).alias("scheduled_timestamp"),
        to_timestamp(
            col("_event.estimated_timestamp")
        ).alias("estimated_timestamp"),
        col("_event.delay_seconds").alias("delay_seconds"),
        to_timestamp(
            col("_event.event_timestamp")
        ).alias("event_timestamp"),
        col("_event._corrupt_record").alias("_corrupt_record"),
    )

    return (
        typed_dataframe
        .withColumn(
            "delay_minutes",
            col("delay_seconds") / lit(60.0),
        )
        .withColumn(
            "validation_error",
            when(
                col("_corrupt_record").isNotNull(),
                lit("invalid_json_or_schema"),
            )
            .when(
                col("event_id").isNull()
                | (length(trim(col("event_id"))) == 0),
                lit("missing_event_id"),
            )
            .when(
                col("trip_id").isNull()
                | (length(trim(col("trip_id"))) == 0),
                lit("missing_trip_id"),
            )
            .when(
                col("event_timestamp").isNull(),
                lit("invalid_or_missing_event_timestamp"),
            )
            .when(
                col("delay_seconds").isNull(),
                lit("invalid_or_missing_delay_seconds"),
            )
            .otherwise(lit(None).cast("string")),
        )
        .drop("_event", "_corrupt_record")
    )