from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType
)
import logging


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("NRB-LoanPipeline")


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("NRB-LoanPipeline")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.sql.adaptive.enabled",
        "true"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Loan Event Schema
# =========================================================

loan_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("bfi_code", StringType(), True),

    StructField(
        "applicant_income",
        DoubleType(),
        True
    ),

    StructField(
        "coapplicant_income",
        DoubleType(),
        True
    ),

    StructField(
        "loan_amount",
        DoubleType(),
        True
    ),

    StructField(
        "credit_score",
        IntegerType(),
        True
    ),

    StructField(
        "collateral_value",
        DoubleType(),
        True
    ),

    StructField(
        "timestamp",
        LongType(),
        True
    )
])


# =========================================================
# Kafka Configuration
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "nrb_loans"


# =========================================================
# Kafka Streaming Source
# =========================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


# =========================================================
# Parse Kafka JSON
# =========================================================

loan_stream = (
    raw_stream

    .select(
        col("key")
        .cast("string")
        .alias("kafka_key"),

        col("value")
        .cast("string")
        .alias("json_value"),

        col("timestamp")
        .alias("kafka_timestamp")
    )

    .withColumn(
        "loan",
        from_json(
            col("json_value"),
            loan_schema
        )
    )

    .select(
        "kafka_key",
        "kafka_timestamp",
        "loan.*"
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
)


print(
    "Stream Driver Status:",
    loan_stream.isStreaming
)

logger.info(
    "Kafka loan streaming source initialized successfully."
)
# =========================================================
# Bronze Delta Layer
# =========================================================

BRONZE_PATH = "data/delta/bronze/loans"

BRONZE_CHECKPOINT = (
    "checkpoints/bronze/loans"
)


bronze_query = (
    loan_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        BRONZE_CHECKPOINT
    )
    .trigger(
        processingTime="5 seconds"
    )
    .start(BRONZE_PATH)
)


logger.info(
    "Bronze Delta streaming query started."
)


bronze_query.awaitTermination()
