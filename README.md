# EMR Athena Partition Performance

## Overview
This project is meant to demonstrate how to build a partitioned data lake on AWS to reduce Athena query cost and improve performance. To accomplish this, synthetic raw transaction CSV data is generated and transformed into partitioned Parquet using PySpark on Amazon EMR, then it is queried via Athena with partition pruning.

## Architecture
```
Raw Layer
S3 (CSV files)
        ↓
Processing Layer
EMR Spark Cluster
        ↓
Curated Layer
S3 Parquet Partitioned by dt
        ↓
Metadata Layer
Glue Data Catalog
        ↓
Query Layer
Athena
```

## Dataset
- 10,000,032 rows of synthetic retail transactions
- 3 months of data: October–December 2025 (92 days)
- Schema: `transactionid`, `transactionts`, `storeid`, `productid`, `amount`
- Partitioned by `dt` (date derived from timestamp)

## Results

All queries run against 10M rows stored as Snappy-compressed Parquet partitioned by `dt`.

| Query | SQL | Run Time | Data Scanned | vs Full Scan |
|---|---|---|---|---|
| Full scan | `SELECT COUNT(*), SUM(amount) FROM transactions` | 1.84s | 45.85 MB | baseline |
| Single day | `WHERE dt = '2025-10-15'` | 618ms | 510.49 KB | **98.9% less** |
| One month | `WHERE dt >= '2025-11-01' AND dt <= '2025-11-30'` | 1.15s | 14.95 MB | **67.4% less** |

Partition pruning means Athena skips S3 files entirely for dates outside the filter — reducing both query cost ($5/TB scanned in Athena) and run time.

## How to Run

**Step 1 — Generate and upload raw data**
```bash
pip install boto3 pandas numpy
python generate_raw_csv.py
```

**Step 2 — Upload PySpark script to S3**
```bash
aws s3 cp transform_to_parquet.py s3://your-bucket/scripts/transform_to_parquet.py
```

**Step 3 — Run EMR cluster**
- Create EMR 6.15.0 cluster with Spark 3.4.1
- Add Spark step: client mode, point to script in S3
- Cluster auto-terminates after step completes

**Step 4 — Create Glue database and register the table**

In Athena, create the database:
```sql
CREATE DATABASE IF NOT EXISTS lakehouse_db;
```

Then create the table pointing to the curated S3 path:
```sql
CREATE EXTERNAL TABLE transactions (
  transactionid STRING,
  transactionts TIMESTAMP,
  storeid       STRING,
  productid     STRING,
  amount        DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://your-bucket/curated/transactions/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

**Step 5 — Register partitions**
```sql
MSCK REPAIR TABLE transactions;
```

## Tech Stack
- **AWS EMR** — managed Spark cluster for distributed transformation
- **Apache Spark / PySpark** — distributed data processing
- **AWS S3** — raw and curated data lake storage
- **AWS Glue Data Catalog** — schema and partition registry
- **Amazon Athena** — serverless SQL query engine
- **Python / boto3** — data generation and S3 upload
