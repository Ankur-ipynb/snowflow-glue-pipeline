import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from src.glue_snowflake_pipeline import process_data, create_broadcast_variable
from datetime import datetime

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestSnowflakeGluePipeline") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_process_data(spark):
    """Test data processing logic"""
    # Create sample data
    transaction_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DoubleType(), True)
    ])
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    transactions = [
        ("C001", datetime(2025, 1, 1), 100.0),
        ("C002", datetime(2025, 1, 2), None)
    ]
    customers = [("C001", "John Doe"), ("C002", "Jane Smith")]
    transaction_df = spark.createDataFrame(transactions, transaction_schema)
    customer_df = spark.createDataFrame(customers, customer_schema)

    # Create broadcast variable and accumulators
    broadcast_var = create_broadcast_variable(spark)
    invalid_records = spark.sparkContext.accumulator(0)
    processed_records = spark.sparkContext.accumulator(0)
    job_args = {"batch_date": "2025-01-01"}

    # Process data
    processed_df = process_data(transaction_df, customer_df, broadcast_var, invalid_records, processed_records, job_args)

    # Validate results
    result = processed_df.collect()
    assert len(result) == 2
    assert result[0]["customer_category"] == "Gold"
    assert result[1]["customer_category"] == "Silver"
    assert invalid_records.value == 1
    assert processed_records.value == 1

def test_broadcast_variable(spark):
    """Test broadcast variable creation"""
    broadcast_var = create_broadcast_variable(spark)
    assert broadcast_var.value["C001"] == "Gold"
    assert broadcast_var.value["C003"] == "Bronze"