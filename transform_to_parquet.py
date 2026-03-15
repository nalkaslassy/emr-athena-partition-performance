"""
Step 2 of 2 — PySpark transform script, runs on Amazon EMR

Reads raw CSV files from S3, cleans and casts the columns,
derives a date partition column (dt) from the timestamp,
then writes the result as partitioned Parquet back to S3.

Upload this file to S3 before running:
    aws s3 cp transform_to_parquet.py s3://your-bucket/scripts/transform_to_parquet.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys

# TODO: replace with your bucket paths
RAW_PATH     = "s3://your-bucket/raw/transactions/*.csv"
CURATED_PATH = "s3://your-bucket/curated/transactions/"

# Read the raw CSV with an explicit schema (avoids Spark inferring wrong types)
raw_schema = StructType([
    StructField("transactionid", StringType(), nullable=False),
    StructField("transactionts",  StringType(), nullable=False),
    StructField("storeid",        StringType(), nullable=False),
    StructField("productid",      StringType(), nullable=False),
    StructField("amount",         StringType(), nullable=False),
])

def main():
    spark = SparkSession.builder \
        .appName("transactions-raw-to-curated") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read
    print(f"Reading CSV from {RAW_PATH}")
    df = spark.read.option("header", "true").schema(raw_schema).csv(RAW_PATH)
    print(f"Row count: {df.count():,}")

    # Cast columns to correct types and derive the partition column
    df = df \
        .withColumn("transactionts", F.to_timestamp("transactionts", "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("amount", F.col("amount").cast(DoubleType())) \
        .withColumn("dt", F.date_format("transactionts", "yyyy-MM-dd")) \
        .filter(F.col("transactionts").isNotNull()) \
        .filter(F.col("amount").isNotNull())

    # Quick sanity check before writing
    null_dt = df.filter(F.col("dt").isNull()).count()
    if null_dt > 0:
        print(f"ERROR: {null_dt} rows have a null dt — check the timestamp format")
        sys.exit(1)

    print(f"Distinct partitions (dt values): {df.select('dt').distinct().count()}")

    # Write as Parquet partitioned by dt
    print(f"Writing partitioned Parquet to {CURATED_PATH}")
    df.repartition("dt") \
      .write \
      .mode("overwrite") \
      .partitionBy("dt") \
      .parquet(CURATED_PATH)

    print("Done. Run MSCK REPAIR TABLE in Athena to register the new partitions.")
    spark.stop()

if __name__ == "__main__":
    main()
