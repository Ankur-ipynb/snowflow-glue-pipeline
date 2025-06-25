import sys
import logging
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, concat, rand
from pyspark.sql.types import StringType
from pyspark.storagelevel import StorageLevel
import boto3
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# Configure logging for Glue job monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name="us-east-1"):
    """Retrieve secret from AWS Secrets Manager"""
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        logger.info(f"Retrieved secret {secret_name}")
        return secret
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        raise

def create_glue_context():
    """Initialize GlueContext and Spark session for AWS Glue"""
    try:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = (glue_context.spark_session.builder
                 .appName("SnowflakeGluePipeline")
                 .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1,org.apache.hadoop:hadoop-aws:3.3.1")
                 .config("spark.hadoop.fs.s3a.access.key", boto3.Session().get_credentials().access_key)
                 .config("spark.hadoop.fs.s3a.secret.key", boto3.Session().get_credentials().secret_key)
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.driver.memory", "4g")
                 .config("spark.executor.memory", "4g")
                 .config("spark.executor.cores", "2")
                 .config("spark.sql.adaptive.enabled", "true")
                 .config("spark.sql.shuffle.partitions", "16")
                 .config("spark.sql.parquet.compression.codec", "snappy")
                 .config("spark.io.compression.codec", "snappy")
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .config("spark.kryo.registrationRequired", "true")
                 .config("spark.kryo.classesToRegister", "org.apache.spark.sql.Row")
                 .config("spark.memory.fraction", "0.6")
                 .config("spark.memory.storageFraction", "0.5")
                 .config("spark.memory.offHeap.enabled", "true")
                 .config("spark.memory.offHeap.size", "1g")
                 .config("spark.shuffle.file.buffer", "64k")
                 .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
                 .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                 .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                 .config("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
                 .getOrCreate())
        logger.info("GlueContext and Spark session created")
        return glue_context, spark
    except Exception as e:
        logger.error(f"Failed to create GlueContext: {str(e)}")
        raise

def get_snowflake_parameters(secret_id):
    """Retrieve Snowflake parameters from Secrets Manager"""
    try:
        secret = get_secret(secret_id)
        params = {
            "sfURL": secret["snowflake_url"],
            "sfUser": secret["user"],
            "sfPassword": secret["password"],
            "sfDatabase": secret["database"],
            "sfSchema": secret["schema"],
            "sfWarehouse": secret["warehouse"],
            "sfRole": secret["role"]
        }
        logger.info("Snowflake parameters retrieved")
        return params
    except Exception as e:
        logger.error(f"Failed to retrieve Snowflake parameters: {str(e)}")
        raise

def create_broadcast_variable(spark):
    """Create a broadcast variable for customer categories"""
    try:
        customer_categories = {
            "C001": "Gold",
            "C002": "Silver",
            "C003": "Bronze"
        }
        broadcast_var = spark.sparkContext.broadcast(customer_categories)
        logger.info("Broadcast variable created")
        return broadcast_var
    except Exception as e:
        logger.error(f"Failed to create broadcast variable: {str(e)}")
        raise

def create_accumulators(spark):
    """Initialize accumulators for tracking invalid and processed records"""
    try:
        invalid_records = spark.sparkContext.accumulator(0)
        processed_records = spark.sparkContext.accumulator(0)
        logger.info("Accumulators initialized")
        return invalid_records, processed_records
    except Exception as e:
        logger.error(f"Failed to initialize accumulators: {str(e)}")
        raise

def read_data_from_snowflake(spark, query, params):
    """Read data from Snowflake with predicate pushdown"""
    try:
        df = spark.read \
            .format("snowflake") \
            .option("sfURL", params["sfURL"]) \
            .option("sfUser", params["sfUser"]) \
            .option("sfPassword", params["sfPassword"]) \
            .option("sfDatabase", params["sfDatabase"]) \
            .option("sfSchema", params["sfSchema"]) \
            .option("sfWarehouse", params["sfWarehouse"]) \
            .option("sfRole", params["sfRole"]) \
            .option("query", query) \
            .option("parallelism", 16) \
            .option("use_arrow", "true") \
            .load()
        logger.info(f"Successfully read data with query: {query}")
        return df
    except Exception as e:
        logger.error(f"Failed to read from Snowflake: {str(e)}")
        raise

def read_customer_data(spark, params):
    """Read customer data from Snowflake"""
    try:
        query = """
        SELECT customer_id, customer_name
        FROM customers
        WHERE active = true
        """
        df = spark.read \
            .format("snowflake") \
            .option("sfURL", params["sfURL"]) \
            .option("sfUser", params["sfUser"]) \
            .option("sfPassword", params["sfPassword"]) \
            .option("sfDatabase", params["sfDatabase"]) \
            .option("sfSchema", params["sfSchema"]) \
            .option("sfWarehouse", params["sfWarehouse"]) \
            .option("sfRole", params["sfRole"]) \
            .option("query", query) \
            .option("parallelism", 16) \
            .option("use_arrow", "true") \
            .load()
        logger.info("Successfully read customer data")
        return df
    except Exception as e:
        logger.error(f"Failed to read customer data: {str(e)}")
        raise

def process_data(df, customer_df, broadcast_var, invalid_records, processed_records, job_args):
    """Process data with salting, joins, and transformations"""
    try:
        df.persist(StorageLevel.MEMORY_AND_DISK)
        def get_category(customer_id):
            return broadcast_var.value.get(customer_id, "Unknown")
        from pyspark.sql.functions import udf
        category_udf = udf(get_category, StringType())
        salted_df = df.withColumn("salt", concat(col("customer_id"), lit("_"), (rand() * 10).cast("int")))
        batch_date = job_args.get("batch_date", "2025-01-01")
        processed_df = (salted_df
                       .join(customer_df, "customer_id", "inner")
                       .filter(col("transaction_date") >= lit(batch_date))
                       .withColumn("processing_timestamp", current_timestamp())
                       .withColumn("status", lit("PROCESSED"))
                       .withColumn("customer_category", category_udf(col("customer_id")))
                       .withColumn("is_valid", when(col("amount").isNotNull(), 1).otherwise(0))
                       .repartition(16, "salt")
                       .select("customer_id", "customer_name", "transaction_date", "amount", "customer_category", "processing_timestamp", "status"))
        def update_accumulators(row):
            if row["is_valid"] == 0:
                invalid_records.add(1)
            else:
                processed_records.add(1)
        processed_df.foreach(update_accumulators)
        processed_df = processed_df.coalesce(4)
        logger.info("Data processing completed")
        return processed_df
    except Exception as e:
        logger.error(f"Data processing failed: {str(e)}")
        raise
    finally:
        df.unpersist()

def write_to_snowflake(df, table_name, params, mode="append"):
    """Write processed data to Snowflake"""
    try:
        (df.write
         .format("snowflake")
         .option("sfURL", params["sfURL"])
         .option("sfUser", params["sfUser"])
         .option("sfPassword", params["sfPassword"])
         .option("sfDatabase", params["sfDatabase"])
         .option("sfSchema", params["sfSchema"])
         .option("sfWarehouse", params["sfWarehouse"])
         .option("sfRole", params["sfRole"])
         .option("dbtable", table_name)
         .option("parallelism", 16)
         .option("compression", "gzip")
         .option("use_arrow", "true")
         .mode(mode)
         .save())
        logger.info(f"Successfully wrote to Snowflake table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Snowflake: {str(e)}")
        raise

def main():
    """Main pipeline function"""
    try:
        # Get job arguments
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "batch_date", "secret_id"])
        logger.info(f"Job arguments: {args}")

        # Initialize Glue context and Spark session
        glue_context, spark = create_glue_context()

        # Initialize Glue job
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)

        # Get Snowflake parameters
        conn_params = get_snowflake_parameters(args["secret_id"])

        # Create broadcast variable and accumulators
        broadcast_var = create_broadcast_variable(spark)
        invalid_records, processed_records = create_accumulators(spark)

        # Read data from Snowflake
        query = f"""
        SELECT customer_id, transaction_date, amount
        FROM transactions
        WHERE transaction_date >= '{args["batch_date"]}' AND amount > 0
        """
        input_df = read_data_from_snowflake(spark, query, conn_params)
        customer_df = read_customer_data(spark, conn_params)

        # Process data
        processed_df = process_data(input_df, customer_df, broadcast_var, invalid_records, processed_records, args)

        # Write to Snowflake
        write_to_snowflake(processed_df, "processed_transactions", conn_params, mode="overwrite")

        # Log metrics
        logger.info(f"Invalid records count: {invalid_records.value}")
        logger.info(f"Processed records count: {processed_records.value}")

        # Validate output with SQLAlchemy
        engine = create_engine(URL(
            account=conn_params["sfURL"].split('.')[0],
            user=conn_params["sfUser"],
            password=conn_params["sfPassword"],
            database=conn_params["sfDatabase"],
            schema=conn_params["sfSchema"],
            warehouse=conn_params["sfWarehouse"],
            role=conn_params["sfRole"]
        ))
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM processed_transactions").fetchone()
            logger.info(f"Total records in output table: {result[0]}")

        # Commit Glue job
        job.commit()
        logger.info("Glue job completed successfully")
    except Exception as e:
        logger.error(f"Glue job failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# Read from cloud storage
# S3: df = spark.read.parquet("s3a://your-bucket/path/")
# GCS: df = spark.read.parquet("gs://your-bucket/path/")
# ADLS Gen2: df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/")
# Write to cloud storage
# df.write.parquet("s3a://your-bucket/output/")