from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_ROOT = PROJECT_ROOT / "spark" / "resources" / "simulation"
SILVER_PATH = (
    PROJECT_ROOT / "data" / "lakehouse" / "silver" / "trip_delays"
)


def read_reference(spark, filename, id_column, name_column):
    schema = StructType([
        StructField(id_column, StringType(), False),
        StructField(name_column, StringType(), False),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(str(REFERENCE_ROOT / filename))
    )


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("sncf-preview-enrichment")
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
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver = spark.read.format("delta").load(str(SILVER_PATH))
        routes = read_reference(
            spark, "routes.csv", "route_id", "line_name"
        )
        stops = read_reference(
            spark, "stops.csv", "stop_id", "stop_name"
        )

        enriched = (
            silver
            .join(routes, on="route_id", how="left")
            .join(stops, on="stop_id", how="left")
        )

        silver_count = silver.count()
        enriched_count = enriched.count()
        missing_names = enriched.filter(
            col("line_name").isNull() | col("stop_name").isNull()
        ).count()

        print(f"Silver row count: {silver_count}")
        print(f"Enriched row count: {enriched_count}")
        print(f"Rows without a line or stop name: {missing_names}")

        enriched.select(
            "event_id",
            "route_id",
            "line_name",
            "stop_id",
            "stop_name",
            "delay_minutes",
        ).show(truncate=False)

        assert enriched_count == silver_count, (
            "La jointure a ajouté ou perdu des lignes : "
            "vérifier les identifiants des référentiels."
        )
        assert missing_names == 0, (
            "Au moins un identifiant n'a pas de nom associé."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()