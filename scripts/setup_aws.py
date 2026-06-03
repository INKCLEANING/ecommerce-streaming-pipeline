"""
Run once to create the S3 bucket and Athena database.
Usage: python scripts/setup_aws.py
"""
import os
import sys

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "ecommerce-streaming-raw")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DATABASE", "ecommerce")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT_LOCATION", f"s3://{BUCKET}/athena-results/")


def create_s3_bucket():
    s3 = boto3.client("s3", region_name=REGION)
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"[OK] Created S3 bucket: s3://{BUCKET}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"[OK] Bucket already exists: s3://{BUCKET}")
        else:
            print(f"[ERROR] S3: {e}")
            sys.exit(1)


def create_athena_database():
    athena = boto3.client("athena", region_name=REGION)
    try:
        athena.start_query_execution(
            QueryString=f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB}",
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        print(f"[OK] Athena database ready: {ATHENA_DB}")
    except ClientError as e:
        print(f"[ERROR] Athena: {e}")
        sys.exit(1)


def verify_credentials():
    sts = boto3.client("sts")
    try:
        identity = sts.get_caller_identity()
        print(f"[OK] AWS credentials valid — account: {identity['Account']}")
    except ClientError as e:
        print(f"[ERROR] Invalid AWS credentials: {e}")
        sys.exit(1)


if __name__ == "__main__":
    verify_credentials()
    create_s3_bucket()
    create_athena_database()
    print("\nSetup complete. Next steps:")
    print("  1. Copy .env.example to .env and fill in your AWS credentials")
    print("  2. Run: docker compose up -d")
    print(f"  3. Run athena/create_tables.sql in the Athena console (replace YOUR_BUCKET with {BUCKET})")
