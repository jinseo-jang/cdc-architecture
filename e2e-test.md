# End-to-End (E2E) Test Report & Guide

본 문서는 Cloud SQL -> Datastream -> GCS -> Pub/Sub -> Dataflow -> BigQuery (Iceberg) 파이프라인의 **End-to-End 테스트 결과**와 **검증 가이드**입니다.

이 가이드를 따라 직접 파이프라인의 정상 동작 여부를 검증할 수 있습니다.

---

## 🛠 테스트 환경 및 준비

- **Source:** Cloud SQL (PostgreSQL) `products` 테이블
- **Pipeline:** Datastream (`pg-to-gcs-stream-v3`), Dataflow (`pg-to-iceberg-cdc-job-pubsub-v1`)
- **Destination:** BigQuery Iceberg Table (`iceberg_dataset.products_iceberg`)

### 사전 준비 (Shell 변수)

터미널에서 검증 명령어를 실행하기 위해 아래 변수를 설정하세요.

```bash
export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME=${PROJECT_ID}-iceberg-storage
export DATASET_ID=iceberg_dataset
```

---

## Scenario 1: 데이터 삽입 (INSERT)

새로운 데이터를 Cloud SQL에 생성하고, BigQuery에 적재되는지 확인합니다.

### 1. Action (Cloud SQL)

Cloud SQL에 접속하여 데이터를 INSERT 합니다.

```bash
# Cloud SQL 접속 (또는 DBeaver 등 사용)
gcloud sql connect cdc-sql-instance --user=postgres --quiet
```

```sql
-- SQL 실행
INSERT INTO products (id, name, price, updated_at) VALUES (5000, 'Verify_Stream_V3', 55.55, NOW());
INSERT INTO products (id, name, price, updated_at) VALUES (6000, 'Flush_Test', 66.66, NOW());
```

### 2. Verification (GCS)

Datastream이 CDC 이벤트를 캡처하여 GCS에 JSONL 파일로 저장했는지 확인합니다. (약 30~60초 소요)

- **콘솔 경로:** [Google Cloud Console > Cloud Storage > 버킷 선택 > cdc-staging 폴더](https://console.cloud.google.com/storage/browser)
- **검증 명령어:**
  ```bash
  gsutil ls -r "gs://$BUCKET_NAME/cdc-staging/**" | tail -n 5
  ```
- **Evidence (실제 결과):**
  ```text
  Found new files in gs://duper-project-1-iceberg-storage/cdc-staging/cdc-staging/public_products/2026/02/19/:
  - .../4e7b7ae2..._postgresql-cdc_..._2_0.jsonl
  - .../a2ce7a60..._postgresql-cdc_..._0_6.jsonl
  ```

### 3. Verification (BigQuery)

Dataflow가 GCS 파일을 처리하여 Iceberg 테이블에 반영했는지 확인합니다.

- **콘솔 경로:** [Google Cloud Console > BigQuery > SQL Workspace](https://console.cloud.google.com/bigquery)
- **검증 명령어:**
  ```bash
  bq query --use_legacy_sql=false \
    "SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.products_iceberg\` WHERE id IN (5000, 6000)"
  ```
- **Evidence (실제 결과):**
  | id | name | price | updated_at |
  | :--- | :--- | :--- | :--- |
  | 5000 | Verify_Stream_V3 | 55.55 | 2026-02-19 03:15:23 |
  | 6000 | Flush_Test | 66.66 | 2026-02-19 03:16:20 |

---

## Scenario 2: 데이터 변경 (UPDATE)

기존 데이터의 값을 변경하고, BigQuery에 반영되는지 확인합니다.

### 1. Action (Cloud SQL)

```sql
UPDATE products SET price = 77.77, updated_at = NOW() WHERE id = 6000;
```

### 2. Verification (GCS)

`change_type: UPDATE` 이벤트 파일이 생성되었는지 확인합니다.

```bash
# 최근 생성된 파일 확인
gsutil ls -lt -r "gs://$BUCKET_NAME/cdc-staging/**" | head -n 5
```

### 3. Verification (BigQuery)

가격(`price`)이 `66.66`에서 `77.77`로 변경되었는지 확인합니다.

```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.products_iceberg\` WHERE id=6000"
```

- **Evidence (실제 결과):**
  | id | name | price | updated_at |
  | :--- | :--- | :--- | :--- |
  | 6000 | Flush_Test | **77.77** | 2026-02-19 03:18:16 |

---

## Scenario 3: 데이터 삭제 (DELETE)

데이터를 삭제하고, BigQuery 테이블에서도 제거되는지 확인합니다.

### 1. Action (Cloud SQL)

```sql
DELETE FROM products WHERE id = 5000;
```

### 2. Verification (GCS)

`change_type: DELETE` 이벤트 파일이 생성되었는지 확인합니다.

### 3. Verification (BigQuery)

해당 ID 조회가 실패하거나 0건이어야 합니다.

```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.products_iceberg\` WHERE id=5000"
```

- **Evidence (실제 결과):**
  ```text
  Query returned 0 rows.
  ```

---

## Troubleshooting Log: Datastream Recovery

테스트 진행 중 발생한 이슈와 해결 과정입니다.

### 이슈: Stream 인증 실패

- **증상:** Cloud SQL 비밀번호 변경 후 기존 Stream(`pg-to-gcs-stream`)이 `FAILED` 상태로 전환됨.
- **시도:** Connection Profile 비밀번호 업데이트 후 Resume 시도했으나 상태 전이 오류 발생.

### 해결: Stream 재생성 (v3)

- **조치:**
  1.  PostgreSQL에서 새로운 Replication Slot 생성 (`cdc_slot_v3`)
  2.  새로운 Datastream Stream 생성 (`pg-to-gcs-stream-v3`) 이때 **Backfill All** 옵션 활성화.
  3.  Dataflow는 Pub/Sub을 통해 GCS 파일 생성을 감지하므로, Stream이 변경되어도 GCS 경로(`cdc-staging/`)만 동일하면 수정 없이 동작함.

---
