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

## Partition Strategy

The dataset is partitioned by `dt` (a date string derived from the transaction timestamp). This was a deliberate design choice:

- **Why `dt` and not `transactionid`?** Partition keys need low cardinality — a manageable number of distinct values. `transactionid` is unique per row (10M values), which would create 10M partitions and destroy performance. `dt` gives 92 partitions for 3 months of data.
- **Why daily and not monthly?** Daily partitions give Athena finer control over what to skip. A query for a single day skips 91/92 partitions. With monthly partitions, that same query would have to scan the entire month.
- **How it works physically:** PySpark writes each day's data into a separate S3 folder named `dt=YYYY-MM-DD/`. When Athena sees a `WHERE dt = '...'` filter, it only opens the matching folder and ignores the rest entirely — it never reads those files at all.

## IAM Setup

Two IAM roles are required:

- **EMR service role** (`EMR_DefaultRole`) — used by the EMR control plane to provision EC2 instances and manage the cluster
- **EC2 instance profile** — attached to every node in the cluster, controls what the nodes themselves can do

The instance profile needs **S3 write access**, not just read. This matters for two reasons: the PySpark job writes the curated Parquet output to S3, and the EMR log aggregation daemon writes step logs to S3. Without write access, both the job output and all logs silently fail.

**Athena permissions** — Athena queries were run as the `mladmin` IAM user which had broad S3 access. In a production setup, the querying identity would need two specific permissions:
- `s3:GetObject` on the curated data path (`s3://your-bucket/curated/transactions/*`)
- `s3:PutObject` on the Athena results path (`s3://your-bucket/athena-results/*`) — Athena writes every query result to S3 before returning it, so write access on the output location is required

## Issues Encountered

**No logs after step failure** — The EC2 instance profile had `AmazonS3ReadOnlyAccess`. The nodes could read data but couldn't write logs to S3, so every failed step produced zero output. Fixed by attaching `AmazonS3FullAccess` to the instance profile role.

**Spark deploy mode** — The EMR console defaults to "Cluster mode" for Spark steps, which runs the driver on a worker node. Python scripts submitted from S3 don't work in cluster mode — YARN can't properly localize and execute them. The fix is to select "Client mode", which runs the driver on the master node where the S3 path is handled correctly.

**Glue schema type mismatch** — After running `MSCK REPAIR TABLE`, Athena returned a type error on the `amount` column. The Glue crawler had registered partition-level schemas with `amount` as `string`, conflicting with the `DOUBLE` type in the Parquet files. Fixed by dropping the table and recreating it with an explicit DDL statement rather than relying on the crawler.

## Tech Stack
- **AWS EMR** — managed Spark cluster for distributed transformation
- **Apache Spark / PySpark** — distributed data processing
- **AWS S3** — raw and curated data lake storage
- **AWS Glue Data Catalog** — schema and partition registry
- **Amazon Athena** — serverless SQL query engine
- **Python / boto3** — data generation and S3 upload
