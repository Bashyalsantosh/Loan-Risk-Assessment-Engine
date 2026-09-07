"""
NRB Loan Risk Engine
Phase 1: Spark + Kafka + Delta Lake Initialization

Purpose:
    Initialize a production-oriented Spark environment for
    the real-time loan risk pipeline.
"""

import logging
from pyspark.sql import SparkSession


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SPARK_VERSION = "3.5.0"
KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"
)
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.0.0"

APP_NAME = "NRB-Loan-Risk-Engine"


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(APP_NAME)


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

def create_spark_session() -> SparkSession:

    logger.info("Initializing Spark session...")

    spark = (
        SparkSession.builder
        .appName(APP_NAME)

        # Kafka + Delta dependencies
        .config(
            "spark.jars.packages",
            f"{KAFKA_PACKAGE},{DELTA_PACKAGE}"
        )

        # Delta Lake
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # Performance / stability
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )

        .getOrCreate()
    )

    logger.info(
        "Spark session created successfully | Spark version: %s",
        spark.version
    )

    return spark


# ---------------------------------------------------------
# Environment Validation
# ---------------------------------------------------------

def validate_spark_environment(spark: SparkSession) -> None:

    logger.info("Validating Spark environment...")

    # Spark version
    logger.info(
        "Spark Version: %s",
        spark.version
    )

    # Application name
    logger.info(
        "Application Name: %s",
        spark.sparkContext.appName
    )

    # Test Spark execution
    test_df = spark.range(1, 5)

    record_count = test_df.count()

    if record_count != 4:
        raise RuntimeError(
            "Spark execution test failed."
        )

    logger.info(
        "Spark execution test passed | Records: %s",
        record_count
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":

    spark = None

    try:

        spark = create_spark_session()

        validate_spark_environment(spark)

        logger.info(
            "Phase 1 initialization completed successfully."
        )

    except Exception as exc:

        logger.exception(
            "Spark initialization failed: %s",
            exc
        )

        raise

    finally:

        if spark is not None:
            spark.stop()

            logger.info(
                "Spark session stopped."
            )
