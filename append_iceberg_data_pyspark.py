from pyspark.sql import SparkSession
import google.auth
from google.auth.transport.requests import Request
import os

# 1. 환경 설정 (이전과 동일)
project_id = "duper-project-1"
location = "us-central1"
catalog_id = "duper-project-1-iceberg-storage"
namespace = "iceberg_dataset"
table_name = "products_iceberg"


def get_access_token():
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


access_token = get_access_token()

# 2. Spark 세션 (이전과 동일한 설정 유지)
spark = (
    SparkSession.builder.appName("Iceberg_Metadata_Test")
    .config(f"spark.sql.catalog.{catalog_id}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_id}.type", "rest")
    .config(
        f"spark.sql.catalog.{catalog_id}.uri",
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    )
    .config(
        f"spark.sql.catalog.{catalog_id}.warehouse",
        f"bq://projects/{project_id}/locations/{location}",
    )
    .config(f"spark.sql.catalog.{catalog_id}.token", access_token)
    .config(f"spark.sql.catalog.{catalog_id}.header.x-goog-user-project", project_id)
    .config(
        f"spark.sql.catalog.{catalog_id}.io-impl",
        "org.apache.iceberg.gcp.gcs.GCSFileIO",
    )
    .getOrCreate()
)

# 3. 데이터 추가 삽입 (두 번째 데이터)
print(f"🚀 '{table_name}'에 두 번째 데이터를 삽입합니다...")

spark.sql(
    f"""
    INSERT INTO `{catalog_id}`.{namespace}.{table_name} 
    VALUES (2, 'metadata_test_item', 150.0, current_timestamp())
"""
)

print("✅ 데이터 삽입 완료!")
