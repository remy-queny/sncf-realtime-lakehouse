from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "bronze" / "trip_updates"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("sncf-inspect-bronze")
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

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(
            f"Bronze table not found at: {BRONZE_PATH}"
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_dataframe = spark.read.format("delta").load(str(BRONZE_PATH))

        row_count = bronze_dataframe.count()

        print(f"Bronze Delta path: {BRONZE_PATH}")
        print(f"Bronze row count: {row_count}")
        print("Bronze schema:")
        bronze_dataframe.printSchema()

        print("Bronze records:")
        (
            bronze_dataframe
            .orderBy("kafka_partition", "kafka_offset")
            .show(20, truncate=False)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()