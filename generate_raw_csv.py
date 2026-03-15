"""
Step 1 of 2 — Generate raw CSV data and upload to S3

Generates 10M synthetic retail transactions across Oct–Dec 2025.
Uploads 3 CSV files (~3.3M rows each) to the S3 raw zone.

Note: Raw CSV has NO dt column. PySpark will derive it from transactionts.

Requirements: pip install boto3 pandas numpy
Run:          python generate_raw_csv.py
"""

import boto3
import pandas as pd
import numpy as np
import io
import random
import string
from datetime import date, timedelta

# ── Config ─────────────────────────────────────────────────────────────────
BUCKET       = "bucket-name-here" # TODO: replace with your actual bucket name
RAW_PREFIX   = "raw/transactions" #TODO: adjust if you want a different S3 prefix
ROWS_PER_DAY = 108_696  # 108,696 rows/day × 92 days ≈ 10M total

MONTHS = [
    ("oct_2025", date(2025, 10, 1), 31),
    ("nov_2025", date(2025, 11, 1), 30),
    ("dec_2025", date(2025, 12, 1), 31),
]

STORE_IDS   = [f"STORE_{i:03d}" for i in range(1, 51)]    # 50 stores
PRODUCT_IDS = [f"PROD_{i:04d}" for i in range(1, 201)]    # 200 products
# ───────────────────────────────────────────────────────────────────────────

s3 = boto3.client("s3")

def make_transaction_id():
    return "TXN" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

def generate_month(filename, start_date, num_days):
    """Generate all rows for one month and return as a DataFrame."""
    print(f"\nGenerating {filename} ({num_days} days × {ROWS_PER_DAY:,} rows)...")
    daily_frames = []

    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        daily_frames.append(pd.DataFrame({
            "transactionid": [make_transaction_id() for _ in range(ROWS_PER_DAY)],
            "transactionts": [
                f"{date_str}T{h:02d}:{m:02d}:{s:02d}"
                for h, m, s in zip(
                    np.random.randint(0, 24, ROWS_PER_DAY),
                    np.random.randint(0, 60, ROWS_PER_DAY),
                    np.random.randint(0, 60, ROWS_PER_DAY),
                )
            ],
            "storeid":   np.random.choice(STORE_IDS,   ROWS_PER_DAY),
            "productid": np.random.choice(PRODUCT_IDS, ROWS_PER_DAY),
            "amount":    np.round(np.random.uniform(1.0, 500.0, ROWS_PER_DAY), 2),
        }))

    return pd.concat(daily_frames, ignore_index=True)

def upload_to_s3(df, filename):
    """Serialize DataFrame to CSV and upload to S3."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    s3_key = f"{RAW_PREFIX}/{filename}.csv"
    size_mb = len(csv_bytes) / (1024 ** 2)
    print(f"  Uploading {size_mb:.1f} MB to s3://{BUCKET}/{s3_key} ...")
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=csv_bytes)
    print(f"  Done. {len(df):,} rows uploaded.")

# ── Main ───────────────────────────────────────────────────────────────────
total = 0
for filename, start_date, num_days in MONTHS:
    df = generate_month(filename, start_date, num_days)
    upload_to_s3(df, filename)
    total += len(df)

print(f"\nAll done. {total:,} total rows uploaded to s3://{BUCKET}/{RAW_PREFIX}/")
print("Next: run transform_to_parquet.py on EMR.")
