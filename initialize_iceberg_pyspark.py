from pyspark.sql import SparkSession
import google.auth
from google.auth.transport.requests import Request
import os

# 1. 환경 설정
project_id = "duper-project-1"
location = "us-central1"
catalog_id = "duper-project-1-iceberg-storage"
bucket_name = "duper-project-1-iceberg-storage"
namespace = "iceberg_dataset"
table_name = "products_iceberg"


# 2. 인증 토큰 획득
def get_access_token():
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


access_token = get_access_token()

# 3. Spark 세션 빌더 (BigLake REST Catalog 설정)
# 🚀 핵심: warehouse를 'bq://'로 설정하면 BigQuery와 자동 연동됩니다.
spark = (
    SparkSession.builder.appName("BigLake_Iceberg_Setup")
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
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

# 4. 네임스페이스 및 테이블 생성 SQL 실행
print(f"🚀 네임스페이스 {namespace} 생성 중...")
# 💡 catalog_id를 백틱(`)으로 감싸야 합니다.
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog_id}`.{namespace}")

print(f"🚀 테이블 {table_name} 생성 중...")
# 💡 여기도 catalog_id를 백틱(`)으로 감싸줍니다.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{catalog_id}`.{namespace}.{table_name} (
        id bigint,
        name string,
        price double,
        updated_at timestamp
    ) 
    USING iceberg
    LOCATION 'gs://{bucket_name}/{namespace}/{table_name}'
"""
)

# 5. 테스트 데이터 삽입
print("🚀 데이터 삽입 중...")
# 💡 여기도 백틱 추가!
spark.sql(
    f"""
    INSERT INTO `{catalog_id}`.{namespace}.{table_name} 
    VALUES (1, 'pyspark_test_item', 99.9, current_timestamp())
"""
)

print(
    f"✅ 모든 작업 완료! 이제 BigQuery 콘솔에서 '{namespace}.{table_name}'을 확인하세요."
)
