import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
import json
import logging
import sys
import time
import pyarrow as pa
import google.auth
import google.auth.transport.requests
from datetime import datetime
from pyiceberg.catalog import load_catalog

# 기존 설정을 지우고 아래로 교체해 보세요.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # 출력을 표준 출력으로 강제
    force=True,  # 기존의 다른 로깅 설정을 무시하고 이 설정을 덮어씌움
)


class IcebergCoWFn(beam.DoFn):
    def __init__(self, project_id, region, catalog_id, table_id):
        self.params = {
            "project_id": project_id,
            "region": region,
            "catalog_id": catalog_id,
            "table_id": table_id,
        }
        # 번들 단위로 데이터를 모으기 위한 저장소
        self.batch_to_upsert = []
        self.ids_to_delete = []
        # 스키마 미리 정의
        self.schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("price", pa.float64()),
                ("updated_at", pa.timestamp("us", tz="UTC")),
            ]
        )

    def setup(self):
        logging.info("--- 인증 및 Catalog 연결 (Retry 설정 포함) ---")
        try:
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            credentials.refresh(google.auth.transport.requests.Request())

            self.catalog = load_catalog(
                "default",
                **{
                    "type": "rest",
                    "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                    "warehouse": f"bq://projects/{self.params['project_id']}/locations/{self.params['region']}",
                    "prefix": f"projects/{self.params['project_id']}/locations/{self.params['region']}/catalogs/{self.params['catalog_id']}",
                    "token": credentials.token,
                    "header.X-Goog-User-Project": self.params["project_id"],
                    # [핵심 추가] 충돌 시 자동 재시도 설정
                    "commit.retry.num-retries": "10",
                    "commit.retry.min-wait-ms": "1000",
                    "commit.retry.max-wait-ms": "10000",
                },
            )
            self.table = self.catalog.load_table(self.params["table_id"])
        except Exception as e:
            logging.error(f"Setup 에러: {e}")
            raise e

    def start_bundle(self):
        # 묶음(Bundle)이 시작될 때 리스트 초기화
        self.batch_to_upsert = []
        self.ids_to_delete = []

    def process(self, line):
        try:
            msg = json.loads(line)
            metadata = msg.get("source_metadata", {})
            change_type = metadata.get("change_type")
            payload = msg.get("payload", {})
            row_id = payload.get("id")

            if not row_id:
                return

            if change_type == "DELETE":
                self.ids_to_delete.append(row_id)
            elif change_type in ["INSERT", "UPDATE"]:
                # 타입 변환
                if payload.get("price") is not None:
                    payload["price"] = float(payload["price"])
                if payload.get("updated_at") is not None:
                    payload["updated_at"] = datetime.fromisoformat(
                        payload["updated_at"].replace("Z", "+00:00")
                    )

                self.ids_to_delete.append(
                    row_id
                )  # 수정을 위해 기존 ID는 삭제 목록에 추가
                self.batch_to_upsert.append(payload)
        except Exception as e:
            logging.error(f"데이터 파싱 에러: {e}")

    def finish_bundle(self):
        if not self.ids_to_delete and not self.batch_to_upsert:
            return

        logging.info(
            f">>> 트랜잭션 시작: 삭제 {len(self.ids_to_delete)}건, 삽입 {len(self.batch_to_upsert)}건"
        )

        try:
            # [핵심 추가] 트랜잭션 시작 전에 테이블 메타데이터를 최신으로 갱신합니다.
            self.table = self.table.refresh()

            with self.table.transaction() as txn:
                # 1. 삭제 처리 (중복 제거된 ID 리스트 사용)
                for rid in set(self.ids_to_delete):
                    txn.delete(f"id == {rid}")

                # 2. 삽입(Append) 처리
                if self.batch_to_upsert:
                    arrow_table = pa.Table.from_pylist(
                        self.batch_to_upsert, schema=self.schema
                    )
                    txn.append(arrow_table)

            logging.info(">>> 트랜잭션 커밋 성공")
        except Exception as e:
            # 만약 충돌이 나더라도 다음 번들에서 다시 시도할 수 있도록 에러를 기록합니다.
            logging.error(f"트랜잭션 커밋 에러: {e}")
            # 리스트를 비우지 않고 유지하면 재시도 로직을 탈 수 있지만,
            # Beam의 특성상 여기서는 raise를 던져 워커가 재시작하게 하는 것이 더 안전할 수 있습니다.
            raise e


def run():
    PROJECT_ID = "duper-project-1"
    REGION = "us-central1"
    BUCKET_NAME = f"{PROJECT_ID}-iceberg-storage"
    START_TIME = time.time()

    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region=REGION,
        temp_location=f"gs://{BUCKET_NAME}/temp",
    )

    # Get subscription name from options or default
    subscription_id = f"projects/{PROJECT_ID}/subscriptions/cdc-stream-sub"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read Pub/Sub"
            >> beam.io.ReadFromPubSub(subscription=subscription_id)
            | "Parse Notification"
            >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))
            | "Filter JSONL"
            >> beam.Filter(
                lambda x: x.get("name", "").endswith(".jsonl")
                and "/cdc-staging/" in x.get("name", "")
            )
            | "Create GCS URI"
            >> beam.Map(lambda x: f"gs://{x['bucket']}/{x['name']}")
            | "Read JSONL" >> beam.io.ReadAllFromText()
            | "To Lines"
            # ReadAllFromText already returns lines, so we don't need to splitlines again.
            # But just in case some lines have internal newlines or we need to ensure flat structure:
            >> beam.FlatMap(lambda line: [line])
            | "Iceberg CoW"
            >> beam.ParDo(
                IcebergCoWFn(
                    PROJECT_ID,
                    REGION,
                    f"{PROJECT_ID}-iceberg-storage",
                    "iceberg_dataset.products_iceberg",
                )
            )
        )


if __name__ == "__main__":
    run()
