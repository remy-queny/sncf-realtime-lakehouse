from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_PATH = PROJECT_ROOT / "data" / "lakehouse" / "_smoke_test_delta"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("sncf-delta-smoke-test")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        dataframe = spark.createDataFrame(
            [
                ("spark_ready", 1),
                ("delta_ready", 1),
                ("java_version", 17),
            ],
            ["check_name", "check_value"],
        )

        dataframe.write.format("delta").mode("overwrite").save(str(OUTPUT_PATH))

        row_count = spark.read.format("delta").load(str(OUTPUT_PATH)).count()

        print(f"Delta table path: {OUTPUT_PATH}")
        print(f"Delta row count: {row_count}")
        print("Delta Lake smoke test completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()