```python
"""
Medallion Pipeline for NRB Loan Risk Assessment Engine

Architecture:
    Bronze → Silver → Gold → ML Risk Assessment

Bronze:
    Parquet Streaming Ingestion

Silver:
    Data Cleansing
    DTI Calculation
    LTV Calculation
    Feature Engineering

Gold:
    ML Risk Assessment
    GBT Classifier
    Model Evaluation
    Model Persistence
"""

import builtins
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    round as sp_round,
    current_timestamp,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    DoubleType,
    IntegerType,
    StringType
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# =========================================================
# Python Native Round
# =========================================================

# Spark को round function सँग conflict नहोस् भनेर
# Python native round लाई अलग reference गरिएको छ।
py_round = builtins.round


# =========================================================
# Spark Initialization
# =========================================================

def init_spark():
    """
    Spark Session Initialization
    """

    spark = (
        SparkSession.builder
        .appName("NRB-Loan-Risk-Engine")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# Bronze Layer
# =========================================================

def run_bronze_layer(spark):
    """
    Bronze Layer:
    Raw loan streaming data ingestion.
    """

    print("\n[🥉] Starting Bronze Streaming Ingestion...")

    schema = StructType([
        StructField(
            "timestamp",
            TimestampType(),
            True
        ),

        StructField(
            "value",
            LongType(),
            True
        ),

        StructField(
            "ingested_at",
            TimestampType(),
            True
        )
    ])

    bronze_stream = (
        spark.readStream
        .schema(schema)
        .format("parquet")
        .load("/tmp/parquet/bronze_loans")
    )

    return bronze_stream


# =========================================================
# Silver Layer
# =========================================================

def run_silver_layer(bronze_stream):
    """
    Silver Layer:
    Data cleansing and feature engineering.

    Features:
        - Total Income
        - Loan-to-Income Ratio
        - Loan-to-Value Ratio
        - Credit Score Validation
    """

    print(
        "[🥈] Running Silver Transformations "
        "& Feature Engineering..."
    )

    enriched = (
        bronze_stream

        # Synthetic loan ID generation
        .withColumn(
            "loan_id",
            expr("concat('L-NRB-', value)")
        )

        # Applicant income
        .withColumn(
            "applicant_income",
            (
                expr("value % 150000") + 35000
            ).cast(DoubleType())
        )

        # Co-applicant income
        .withColumn(
            "coapplicant_income",
            expr("value % 50000").cast(DoubleType())
        )

        # Loan amount
        .withColumn(
            "loan_amount",
            (
                expr("value % 2000000") + 100000
            ).cast(DoubleType())
        )

        # Synthetic credit score
        .withColumn(
            "credit_score",
            (
                expr("value % 350") + 500
            ).cast(IntegerType())
        )

        # Collateral value
        .withColumn(
            "collateral_value",
            (
                expr("value % 4000000") + 500000
            ).cast(DoubleType())
        )
    )

    silver_df = (
        enriched

        # Total household income
        .withColumn(
            "total_income",
            col("applicant_income")
            + col("coapplicant_income")
        )

        # Loan-to-Income ratio
        #
        # NOTE:
        # This is not standard DTI.
        # Standard DTI requires debt obligations /
        # monthly income.
        .withColumn(
            "dti_ratio",
            sp_round(
                col("loan_amount")
                / col("total_income"),
                4
            )
        )

        # Loan-to-Value ratio
        .withColumn(
            "ltv_ratio",
            sp_round(
                col("loan_amount")
                / col("collateral_value"),
                4
            )
        )

        # Basic data quality validation
        .filter(
            (col("applicant_income") > 0)
            &
            (col("loan_amount") > 0)
            &
            (col("credit_score").between(300, 850))
        )

        # Processing timestamp
        .withColumn(
            "processed_at",
            current_timestamp()
        )
    )

    return silver_df


# =========================================================
# Gold Layer — Machine Learning
# =========================================================

def train_gold_ml_model(spark):
    """
    Gold Layer:
    Train a GBT-based loan default risk model.

    NOTE:
    The current label is synthetic/rule-based and should
    NOT be described as an official NRB compliance model.
    """

    print(
        "[🥇] Training Gold Layer "
        "Machine Learning Model..."
    )

    # -----------------------------------------------------
    # Read Silver Dataset
    # -----------------------------------------------------

    silver_path = "/tmp/parquet/silver_loans"

    silver_df = (
        spark.read
        .format("parquet")
        .load(silver_path)
    )

    # -----------------------------------------------------
    # Synthetic Default Label
    # -----------------------------------------------------

    labeled_df = (
        silver_df
        .withColumn(
            "is_default",
            when(
                (col("dti_ratio") > 0.45)
                |
                (col("credit_score") < 600)
                |
                (col("ltv_ratio") > 0.80),
                1
            )
            .otherwise(0)
        )
    )

    # -----------------------------------------------------
    # ML Features
    # -----------------------------------------------------

    feature_cols = [
        "applicant_income",
        "total_income",
        "dti_ratio",
        "ltv_ratio",
        "credit_score"
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    ml_dataset = (
        assembler
        .transform(labeled_df)
        .select(
            "features",
            "is_default",
            "loan_id"
        )
    )

    # -----------------------------------------------------
    # Train / Test Split
    # -----------------------------------------------------

    train_data, test_data = (
        ml_dataset
        .randomSplit(
            [0.8, 0.2],
            seed=42
        )
    )

    # -----------------------------------------------------
    # GBT Classifier
    # -----------------------------------------------------

    gbt = GBTClassifier(
        labelCol="is_default",
        featuresCol="features",
        maxDepth=5,
        maxIter=20
    )

    model = gbt.fit(train_data)

    # -----------------------------------------------------
    # Predictions
    # -----------------------------------------------------

    predictions = model.transform(test_data)

    # -----------------------------------------------------
    # Model Evaluation
    # -----------------------------------------------------

    evaluator = BinaryClassificationEvaluator(
        labelCol="is_default",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    auc_score = evaluator.evaluate(
        predictions
    )

    print(
        f"✅ Training Complete! "
        f"Model ROC-AUC: {auc_score:.4f}"
    )

    # -----------------------------------------------------
    # Save Model
    # -----------------------------------------------------

    model_path = "/tmp/models/gbt_loan_model"

    (
        model.write()
        .overwrite()
        .save(model_path)
    )

    print(
        f"💾 Model persisted successfully "
        f"to '{model_path}'"
    )

    return predictions


# =========================================================
# Main Pipeline Driver
# =========================================================

if __name__ == "__main__":

    spark = init_spark()

    print(
        "🚀 NRB Medallion Pipeline Engine Initialized."
    )

    silver_path = "/tmp/parquet/silver_loans"

    # -----------------------------------------------------
    # Train ML model only when Silver data exists
    # -----------------------------------------------------

    if os.path.exists(silver_path):

        train_gold_ml_model(spark)

    else:

        print(
            "⚠️ Silver Storage not found. "
            "Please run the streaming driver "
            "to collect initial batch."
        )

    # -----------------------------------------------------
    # Stop Spark gracefully
    # -----------------------------------------------------

    spark.stop()
```

