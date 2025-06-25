Snowflake Glue Pipeline
A PySpark pipeline for AWS Glue that processes data from Snowflake, applies transformations, and writes back to Snowflake. Integrated with AWS S3, Secrets Manager, and Azure Data Factory (ADF) for SSIS orchestration.
Architecture

Source/Sink: Snowflake for transactions and customer data.
Processing: AWS Glue runs PySpark for ETL (salting, joins, UDFs).
Storage: S3 for intermediate data and dependencies.
Secrets: AWS Secrets Manager for Snowflake credentials.
Orchestration: ADF triggers Glue job and SSIS packages.
CI/CD: GitHub Actions for linting, testing, and deployment.

Prerequisites

AWS Glue (Glue 3.0, Spark 3.1)
Snowflake account
S3 bucket for scripts and JARs
AWS Secrets Manager with Snowflake credentials
ADF with SSIS IR (optional)
Python 3.8+
JARs: snowflake-jdbc-3.13.14.jar, spark-snowflake_2.12-2.10.0-spark_3.1.jar, hadoop-aws-3.3.1.jar

Setup

Clone Repository:
git clone https://github.com/your-org/snowflake-glue-pipeline.git
cd snowflake-glue-pipeline


Install Dependencies:
pip install -r requirements.txt


Upload Jars:

Place JARs in jars/ and upload to S3:aws s3 cp jars/ s3://your-bucket/jars/ --recursive




Configure Secrets:

Create a secret in Secrets Manager (snowflake-credentials):snowflake_url: your_account.snowflakecomputing.com
user: your_username
password: your_password
database: your_database
schema: your_schema
warehouse: your_warehouse
role: your_role




Deploy Glue Job:

Update config/glue_job_config.json with your AWS account details.
Run deployment script:./scripts/deploy_glue_job.sh




ADF Integration:

Create an ADF pipeline with a Web Activity to trigger the Glue job:{
  "url": "https://glue.us-east-1.amazonaws.com/",
  "method": "POST",
  "body": {
    "JobName": "SnowflakeGluePipeline",
    "Arguments": {
      "--batch_date": "@pipeline().parameters.batch_date",
      "--secret_id": "snowflake-credentials"
    }
  }
}


Add an Execute SSIS Package Activity for SQL Server processing.



Running Locally

Install Spark locally and run:spark-submit --jars jars/* src/glue_snowflake_pipeline.py --JOB_NAME test --batch_date 2025-01-01 --secret_id snowflake-credentials



Testing

Run unit tests:pytest tests/test_pipeline.py



CI/CD

GitHub Actions lints, tests, and deploys on push to main.
Configure AWS credentials in GitHub Secrets (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).

Monitoring

Glue Console: View job runs and logs.
CloudWatch: Logs in /aws-glue/jobs/output and /aws-glue/jobs/error.
ADF: Monitor pipeline runs for Glue and SSIS.

Cloud Storage

S3: df = spark.read.parquet("s3a://your-bucket/path/")
GCS: Requires gcs-connector-hadoop3 and service account key.
ADLS Gen2: Requires hadoop-azure and OAuth credentials.

License
MIT License
