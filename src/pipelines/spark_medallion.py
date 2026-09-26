import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkMedallionPipeline")

class MedallionSparkPipeline:
    def __init__(self, spark_session: SparkSession = None):
        self.spark = spark_session or SparkSession.builder \
            .appName("LoanRiskMedallionPipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def process_bronze_to_silver(self, bronze_path: str, silver_path: str):
        """Cleanse raw data, handle nulls, and enforce schema integrity."""
        logger.info("Executing Bronze to Silver Delta Processing...")
        
        bronze_df = self.spark.read.format("delta").load(bronze_path)
        
        silver_df = bronze_df.filter(col("application_id").isNotNull()) \
            .fillna({"dti": 0.5, "ltv": 0.7, "p_instances": 0}) \
            .withColumn("dti_normalized", when(col("dti") > 1.0, 1.0).otherwise(col("dti"))) \
            .withColumn("processed_at", current_timestamp())
            
        silver_df.write.format("delta").mode("append").save(silver_path)
        logger.info("Silver Layer successfully written.")

    def process_silver_to_gold(self, silver_path: str, gold_path: str):
        """Feature engineering and risk scoring calculation ($Rc$)."""
        logger.info("Executing Silver to Gold Feature Store Processing...")
        
        silver_df = self.spark.read.format("delta").load(silver_path)
        
        w1, w2, w3 = 0.40, 0.35, 0.25
        
        gold_df = silver_df.withColumn(
            "rc_score",
            (col("dti_normalized") * lit(w1)) + (col("ltv") * lit(w2)) + ((col("p_instances") / lit(5.0)) * lit(w3))
        ).withColumn(
            "risk_tier",
            when(col("rc_score") >= 0.75, "CRITICAL DEFAULT RISK")
            .when(col("rc_score") >= 0.45, "WATCHLIST ELEVATED")
            .otherwise("PASSABLE LOW RISK")
        )
        
        gold_df.write.format("delta").mode("overwrite").save(gold_path)
        logger.info("Gold Layer successfully generated.")
