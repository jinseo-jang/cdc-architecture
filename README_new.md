# Cloud SQL to BigQuery BigLake Iceberg CDC 구축 가이드

본 가이드는 **Python** 기반의 **Copy-on-Write(CoW)** 로직을 반영하여, Cloud SQL부터 BigQuery BigLake Iceberg 테이블까지 이어지는 전체 CDC 구축 과정을 다룹니다. 실제 운영 환경에서 즉시 적용 가능한 수준으로 구성되었습니다.

---

## 1. 아키텍처 리뷰 및 최적화

- **구조:** `Cloud SQL (PostgreSQL)` → `Datastream` → `GCS (JSONL)` → `Pub/Sub` → `Dataflow (Python/CoW)` → `GCS (Iceberg)` → `BigQuery (BigLake)`
- **핵심 전략:** **Copy-on-Write(CoW)** 및 **Event-Driven 아키텍처**. GCS에 적재되는 이벤트를 Pub/Sub으로 실시간 감지하여 Dataflow로 전달, 불필요한 Polling을 제거하고 응답성을 높였습니다. 추가로 업데이트/삭제 시 데이터 파일을 즉시 재작성하여 BigQuery의 읽기 성능을 극대화합니다.
- **검증:** Datastream의 JSON 출력에는 `_metadata_change_type` 필드가 포함되어 있어, 이를 기반으로 Dataflow에서 `INSERT`, `UPDATE`, `DELETE` 분기 처리가 가능합니다.

---

## 2. 통합 단계별 구축 가이드

### 환경 변수 설정

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export CLOUD_SQL_INST=cdc-sql-instance
export DB_PASSWORD=YOUR_NEW_PASSWORD
export TOPIC_NAME=cdc-stream-topic
export SUBSCRIPTION_NAME=cdc-stream-sub
export BUCKET_NAME=${PROJECT_ID}-iceberg-storage
export DATASET_ID=iceberg_dataset
export CONNECTION_ID=my-iceberg-conn

```

### Step 1: 소스(Cloud SQL) 및 CDC 활성화

1. **인스턴스 생성**

```bash
gcloud sql instances create $CLOUD_SQL_INST \
    --database-version=POSTGRES_16 \
    --tier=db-perf-optimized-N-4 \
    --region=$REGION \
    --edition=ENTERPRISE_PLUS \
    --database-flags=cloudsql.logical_decoding=on,cloudsql.iam_authentication=on

```

2. **postgres 사용자 비밀번호 생성**

```bash
# YOUR_NEW_PASSWORD 부분을 본인이 원하는 비밀번호로 수정하세요.
gcloud sql users set-password postgres \
    --instance=$CLOUD_SQL_INST \
    --password=YOUR_NEW_PASSWORD

```

3. **DB 접속**

```bash
gcloud sql connect $CLOUD_SQL_INST --user=postgres --quiet

```

4. **스키마 및 복제 슬롯 설정** (DB 접속 후 실행)

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC용 Publication 및 Slot 생성
CREATE PUBLICATION cdc_pub FOR TABLE products;

-- postgres 사용자에게 복제 권한 부여 및 슬롯 생성
ALTER USER postgres WITH REPLICATION;
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

-- 슬롯 상태 확인
SELECT slot_name, plugin, active FROM pg_replication_slots;

```

---

### Step 2: Pub/Sub 및 GCS 준비

```bash
# GCS 버킷 생성
gsutil mb -l $REGION gs://$BUCKET_NAME

# Pub/Sub 토픽 및 구독 생성
gcloud pubsub topics create $TOPIC_NAME
gcloud pubsub subscriptions create $SUBSCRIPTION_NAME --topic=$TOPIC_NAME

# GCS 서비스 계정이 Pub/Sub에 메시지를 발행할 수 있도록 권한 부여
export GCS_SA=$(gsutil kms serviceaccount -p $PROJECT_ID)
gcloud pubsub topics add-iam-policy-binding projects/$PROJECT_ID/topics/$TOPIC_NAME \
    --member="serviceAccount:$GCS_SA" \
    --role="roles/pubsub.publisher"

# GCS 버킷의 객체 생성 이벤트를 Pub/Sub 토픽으로 전송하도록 알림(Notification) 구성
gsutil notification create -t $TOPIC_NAME -f json -e OBJECT_FINALIZE gs://$BUCKET_NAME

# 확인
gsutil notification list gs://$BUCKET_NAME
```

---

### Step 3: BigLake 및 Iceberg 테이블 생성

1. **External Connection 생성**

```bash
bq mk --connection --location=$REGION --project_id=$PROJECT_ID \
    --connection_type=CLOUD_RESOURCE $CONNECTION_ID

```

2. **Connection SA에 GCS 권한 부여**

```bash
# Connection의 서비스 계정 추출
export CONNECTION_SA=$(bq show --connection --location=$REGION --format=json $CONNECTION_ID | jq -r '.cloudResource.serviceAccountId')

# GCS 객체 관리 권한 부여
gcloud projects add-iam-policy-binding $PROJECT_ID \
   --member="serviceAccount:$CONNECTION_SA" \
   --role="roles/storage.objectAdmin"

# BigLake Admin 권한 추가 (Catalog 관리 및 Iceberg 연동 필수)
gcloud projects add-iam-policy-binding $PROJECT_ID \
   --member="serviceAccount:$CONNECTION_SA" \
   --role="roles/biglake.admin"

```

---

## Step 3-1: BigLake Rest Catalog 생성 및 Iceberg 테이블 초기화

### 1. BigLake Rest Catalog 생성 (Console)

1. **BigQuery 콘솔** > **BigLake** 페이지로 이동합니다.
2. **[+ Create Catalog]** 를 클릭합니다.
3. **Catalog ID:** `my-rest-catalog`
4. **Storage Bucket:** `gs://duper-project-1-iceberg-storage`
5. **Location:** `us-central1`

### 2. 환경 및 패키지 설정

```bash
# 서비스 활성화
gcloud services enable biglake.googleapis.com --project duper-project-1

# 필요 패키지 설치
pip install pyspark==3.5.4 google-auth

# Java 17 설치 및 환경 설정 (macOS 기준)
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

```

### 3. Iceberg 초기화 실행 (PySpark)

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-gcp-bundle:1.5.2 \
  --conf "spark.driver.extraJavaOptions=-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED" \
  --conf "spark.driver.host=localhost" \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.io.netty.kqueue.enabled=false" \
  initialize_iceberg_pyspark.py

```

### 4. BigQuery 콘솔 최종 확인

- **자동 생성 확인:** `iceberg_dataset` 내에 `products_iceberg` 테이블 생성 여부 확인.
- **세부 정보 확인:** 테이블 상세 정보 하단에 **Open Catalog Table Configuration** 섹션 활성화 여부 확인.
- **데이터 조회 테스트:**

```sql
SELECT * FROM `iceberg_dataset.products_iceberg`;

```

---

### 5. 추가 데이터 생성 테스트 (Append)

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-gcp-bundle:1.5.2 \
  --conf "spark.driver.extraJavaOptions=-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED" \
  --conf "spark.driver.host=localhost" \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.io.netty.kqueue.enabled=false" \
  append_iceberg_data_pyspark.py

```

- **메타데이터 버전 변경 확인:** `metadata_location`이 `00002-xxxx.json`으로 갱신되었는지 확인.

---

## Step 4: Datastream 구성 (연결 프로필 및 스트림)

이 단계에서는 Cloud SQL(PostgreSQL)과 GCS를 잇는 연결 프로필을 먼저 생성한 후, 이를 조합하여 실시간 데이터 스트림을 가동합니다.

### 4.1 소스 연결 프로필 생성 (Source Connection Profile)

Datastream이 Cloud SQL에 접속하기 위한 신분증을 만드는 과정입니다.

1. **메뉴 이동:** `Datastream` > `Connection Profiles` > `[+ CREATE PROFILE]` 클릭 후 **PostgreSQL** 선택
2. **설정 값 입력:**

- **Connection profile name:** `postgres-source-profile`
- **Hostname/IP:** Cloud SQL 인스턴스의 **공용 IP 주소**
- **Port:** `5432`
- **Username / Password:** `postgres` / [설정한 비밀번호]
- **Database:** `postgres`

3. **Connectivity method:** `IP allowlist` 선택 (앞서 Cloud SQL 방화벽에 등록한 5개 IP 대역 사용)
4. **테스트 및 생성:** `RUN TEST`를 눌러 **Passed** 확인 후 `CREATE` 클릭

### 4.2 목적지 연결 프로필 생성 (Destination Connection Profile)

변경 데이터가 저장될 GCS 버킷 정보를 설정합니다.

1. **메뉴 이동:** `Connection Profiles` > `[+ CREATE PROFILE]` 클릭 후 **Cloud Storage** 선택
2. **설정 값 입력:**

- **Connection profile name:** `gcs-dest-profile`
- **Bucket name:** `$BUCKET_NAME` (Step 2에서 생성한 버킷 선택)

3. **생성:** `CREATE` 클릭

### 4.3 스트림 생성 및 실행 (Create & Start Stream)

생성한 두 프로필을 연결하여 실제 데이터 파이프라인을 활성화합니다.

1. **스트림 기본 정보:** 소스(`PostgreSQL`), 대상(`Cloud Storage`) 선택
2. **소스 구성 (Configure source):**

- **Replication slot name:** `pg-to-gcs-stream` (또는 DB에서 생성한 슬롯명)
- **Publication name:** `cdc_pub`
- **Select objects:** `public.products` 테이블 체크

3. **목적지 구성 (Configure destination):**

- **Stream path prefix:** `/cdc-staging`
- **Output format:** **JSON** (Dataflow 처리를 위해 필수)
- **Enable gzip compression:** 해제 (권장)

4. **검증 및 시작:** `RUN VALIDATION`을 통해 모든 항목 초록불 확인 후 `CREATE & START` 클릭

---

## 🔍 실시간 데이터 연동 확인 (Test)

스트림이 **Running** 상태가 되면 아래 테스트를 통해 연동 여부를 최종 확인합니다.

1. **Cloud SQL 데이터 조작:**

```sql
INSERT INTO products (id, name, price) VALUES (888, 'Dataflow_Ready_Test', 99.9);

```

2. **GCS 파일 적재 확인:**

- GCS 버킷 내 `cdc-staging/` 경로에 날짜별 폴더가 생성되었는지 확인합니다.
- 생성된 `.jsonl` 파일을 열어 `change_type: INSERT` 정보가 포함되어 있는지 확인합니다.

---

## Step 5: Dataflow Python (CoW) 파이프라인 구현

이 단계에서는 기존의 10초 주기 Polling 방식을 개선하여, 실시간 GCS 이벤트(Pub/Sub)를 구독하는 파이프라인을 구축합니다.

### 1. 파이프라인 핵심 로직 개요

- **Pub/Sub 감지:** GCS 파일 생성 이벤트 메시지를 파싱하여 버킷 이름과 파일 이름을 추출합니다.
- **JSONL 처리:** 추출된 GCS 경로를 기반으로 파일 내용을 읽고 각 라인을 개별 처리합니다.
- **Upsert (INSERT/UPDATE):** 동일 `id`를 가진 기존 행을 Iceberg에서 삭제한 후 새로운 `payload`를 추가합니다.
- **Delete (DELETE):** `change_type`이 DELETE인 경우 해당 `id`를 테이블에서 제거합니다.

### 2. Python 가상환경 및 패키지 설정

Dataflow 워커와의 호환성을 위해 **Python 3.11** 환경 사용을 권장합니다.

```bash
# 가상환경 생성 및 활성화
python3.11 -m venv venv
source venv/bin/activate

# 의존성 패키지 설치
pip install --upgrade pip
pip install -r requirements.txt
```

### 2-1. `setup.py` 생성

Dataflow 워커(Linux)와 로컬 환경(macOS 등)의 OS 차이로 인한 의존성 설치 에러를 방지하기 위해, 패키지 설치 정보를 담은 `setup.py`를 생성합니다.

```python
# setup.py
import setuptools

setuptools.setup(
    name='cdc-architecture',
    version='0.0.1',
    description='CDC Dataflow Pipeline',
    install_requires=[
        'apache-beam[gcp]',
        'pyiceberg[pyarrow,gcp]',
        'google-auth',
        'requests',
        'pyarrow'
    ],
    packages=setuptools.find_packages(),
)
```

### 3. 파이프라인 실행 및 모니터링

`main_pubsub.py` 스크립트를 통해 로컬(DirectRunner)에서 이벤트를 테스트할 수 있습니다.

```bash
# 로컬에서 실행 테스트
python main_pubsub.py --runner DirectRunner
```

### 4. CDC 검증 SQL

```sql
-- 1. 새로운 데이터 생성
INSERT INTO products (id, name, price, updated_at)
VALUES (1004, 'Final_Validation_Item', 77.7, NOW());

-- 2. 가격 수정 (77.7 -> 88.8)
UPDATE products SET price = 88.8, updated_at = NOW() WHERE id = 1004;

-- 3. 데이터 삭제
DELETE FROM products WHERE id = 1004;
```

### 5. BQ 쿼리 확인

```sql
SELECT * FROM `iceberg_dataset.products_iceberg` WHERE id = 1004;
```

---

## Step 6: 클라우드 배포 준비

### 1 Dataflow 배포 명령어

터미널에서 아래 명령어를 입력하여 클라우드로 배포합니다.
_Pub/Sub 기반으로 변경되었으므로 파이프라인 코드는 주기적 폴링이 아닌 실시간 이벤트 수신에 최적화됩니다._

```bash
python main_pubsub.py \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --region $REGION \
    --temp_location gs://$BUCKET_NAME/temp \
    --staging_location gs://$BUCKET_NAME/staging \
    --streaming \
    --setup_file ./setup.py \
    --job_name pg-to-iceberg-cdc-job-pubsub-v1

```

---

## Step 7: 검증

## 1. 실시간 동기화 확인 및 모니터링

## 1.1 검증 SQL 실행

```sql
-- 1. 새로운 데이터 생성
INSERT INTO products (id, name, price, updated_at)
VALUES (2026, 'Final_Validation_Item', 77.7, NOW());

-- 2. 가격 수정 (77.7 -> 88.8)
UPDATE products SET price = 88.8, updated_at = NOW() WHERE id = 2026;

-- 3. 데이터 삭제
DELETE FROM products WHERE id = 2026;
```

### 1.1 Dataflow 로그 확인

Dataflow 콘솔에서 작업이 정상적으로 수행되는지 실시간 로그를 확인합니다.

1. **메뉴 이동:** Dataflow > Jobs > `pg-to-iceberg-cdc-job-v3` 클릭.
2. **로그 확인:** 하단 **[Logs]** 탭에서 **[Worker Logs (작업자 로그)]** 선택.
3. **성공 지표:** `>>> 트랜잭션 커밋 성공` 메시지가 찍히면 Iceberg 테이블에 반영된 것입니다.

### 1.2 BigQuery 테이블 동기화 확인

BigQuery에서 SQL을 실행하여 최종적으로 데이터가 동기화되었는지 Cloud SQL 데이터와 비교 검증합니다.

```sql
-- 1. 최종 적재 데이터 확인
SELECT * FROM `iceberg_dataset.products_iceberg` ORDER BY updated_at DESC;

```

---
