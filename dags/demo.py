# file: dags/con_ingest_bangkokhospital.py
from __future__ import annotations
from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# ----------------------------
# Airflow Variables
# ----------------------------
MINIO_BUCKET = Variable.get("MINIO_BUCKET")              # ชื่อบัคเก็ต
RAW_PREFIX   = Variable.get("RAW_PREFIX")                # เช่น raw/nectec/bangkokhospital_com/jsonl
CLEAN_PREFIX = Variable.get("CLEAN_PREFIX", "cleaned")
VALID_PREFIX = Variable.get("VALID_PREFIX", "validated")
LOGS_PREFIX  = Variable.get("LOGS_PREFIX", "meta")
PG_DSN       = Variable.get("PG_DSN")                    # postgresql://user:pass@host:port/db

default_args = {
    "owner": "siamai",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="demo",
    description="Clean -> Validate -> Map -> Ingest for bangkokhospital_com",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    doc_md="""
### Pipeline (bangkokhospital_com)
1) ลิสต์ไฟล์ภายใต้ RAW_PREFIX  
2) เลือกไฟล์ (จาก dag_run.conf.raw_key ถ้ามี)  
3) คำนวณพาธ cleaned/validated/mapped  
4) รัน run.py (clean→validate→map→ingest)  
""",
) as dag:

    # 1) List RAW keys (MinIO ผ่าน conn_id 'minio_s3')
    list_raw = S3ListOperator(
        task_id="list_raw_keys",
        bucket=MINIO_BUCKET,
        prefix=RAW_PREFIX.rstrip("/") + "/",
        aws_conn_id="minio_s3",
    )

    # 2) Choose key
    def choose_key_func(ti, **context):
        conf = (context.get("dag_run") or {}).conf or {}
        forced = conf.get("raw_key") #mannual key from dag_run conf
        if forced:
            ti.xcom_push(key="src_key", value=forced)
            return
        keys = ti.xcom_pull(task_ids="list_raw_keys") or []
        keys = [k for k in keys if k.endswith(".jsonl")]
        if not keys:
            raise ValueError("ไม่พบไฟล์ .jsonl ใต้ RAW_PREFIX — โปรดอัปโหลดไฟล์หรือกำหนด dag_run.conf.raw_key")
        ti.xcom_push(key="src_key", value=keys[0])

    choose_key = PythonOperator(
        task_id="choose_key",
        python_callable=choose_key_func,
    )

    # 3) Make paths
    def make_paths_func(ti, raw_prefix, cleaned_prefix, validated_prefix, logs_prefix):
        src_key = ti.xcom_pull(task_ids="choose_key", key="src_key")
        if not src_key or not src_key.startswith(raw_prefix.rstrip("/") + "/"):
            raise ValueError(f"src_key ไม่ขึ้นต้นด้วย {raw_prefix}/: {src_key}")

        base   = src_key.replace(raw_prefix.rstrip("/") + "/", "", 1)
        parent = os.path.dirname(base)
        fname  = os.path.basename(src_key).removesuffix(".jsonl")

        cleaned_key   = f"{cleaned_prefix.rstrip('/')}/{parent}/{fname}.cleaned.jsonl"
        cleanlog_key  = f"{logs_prefix.rstrip('/')}/preprocessing_logs/{parent}/{fname}.cleaned.log.jsonl"
        validated_key = f"{validated_prefix.rstrip('/')}/{parent}/{fname}.validated.jsonl"
        validlog_key  = f"{logs_prefix.rstrip('/')}/validation_logs/{parent}/{fname}.validated.log.jsonl"

        # รูปแบบ mapping ให้ตรงกับ run.py: mapping/{parent}/{fname}.mapped.jsonl
        # mapped_key = f"mapping/{parent}/{fname}.mapped.jsonl"
        parent_no_jsonl = parent.split("/jsonl", 1)[0]                  # 'nectec/bangkokhospital_com'
        mapped_key      = f"mapping/{parent_no_jsonl}/{fname}.mapped.jsonl"
        for k, v in {
            "src_key": src_key,
            "cleaned_key": cleaned_key,
            "cleanlog_key": cleanlog_key,
            "validated_key": validated_key,
            "validlog_key": validlog_key,
            "mapped_key": mapped_key,
        }.items():
            ti.xcom_push(key=k, value=v)

    make_paths = PythonOperator(
        task_id="make_paths",
        python_callable=make_paths_func,
        op_kwargs=dict(
            raw_prefix=RAW_PREFIX,
            cleaned_prefix=CLEAN_PREFIX,
            validated_prefix=VALID_PREFIX,
            logs_prefix=LOGS_PREFIX,
        ),
    )

    # 4) Run run.py (clean→validate→map→ingest)
    #    ใช้ XCom 'src_key' + Variables อื่น ๆ; เปิด --text เป็นค่าปริยาย
    run_pipeline = BashOperator(
        task_id="run_pipeline",
 bash_command=(
        "python3 /opt/airflow/dags/run.py "
        "--bucket {{ var.value.MINIO_BUCKET }} "
        "--src-key {{ ti.xcom_pull('make_paths', key='src_key') }} "
        "--text "
        #"{% if var.value.PG_DSN %}--dsn {{ var.value.PG_DSN }}{% endif %}"
        "--dsn {{ conn.pg_app.get_uri() | replace('postgres://','postgresql://') }}"

    ),
    env={
        "MINIO_ENDPOINT": "http://minio:9000",
        # ถ้าใช้ Airflow Connection: minio_s3
        "MINIO_ACCESS_KEY": "{{ conn.minio_s3.login }}",
        "MINIO_SECRET_KEY": "{{ conn.minio_s3.password }}",
        "MINIO_SECURE": "false",
        "MINIO_BUCKET": "{{ var.value.MINIO_BUCKET }}",
        #"PG_DSN": "{{ var.value.PG_DSN }}",
        # เปิดโหมดดีบักชั่วคราวได้
        # "MINIO_DEBUG": "true",
    },
    )

    list_raw >> choose_key >> make_paths >> run_pipeline