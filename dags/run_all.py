# file: dags/con_ingest_bangkokhospital_all.py
from __future__ import annotations
from datetime import timedelta, datetime
import os
from typing import List, Dict
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor  # NEW
import psycopg
# -----------------------------------------------------

MINIO_BUCKET = Variable.get("MINIO_BUCKET")
RAW_PREFIX   = Variable.get("RAW_PREFIX")
CLEAN_PREFIX = Variable.get("CLEAN_PREFIX", "cleaned")
VALID_PREFIX = Variable.get("VALID_PREFIX", "validated")
LOGS_PREFIX  = Variable.get("LOGS_PREFIX", "meta")
PG_DSN       = Variable.get("PG_DSN")

default_args = {
    "owner": "siamai",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="sensor_demo",
    description="Run run.py for EVERY .jsonl under RAW_PREFIX (dynamic mapping via env SRC_KEY)",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule= "@hourly",
    catchup=False,
) as dag:

    wait_raw_stable = S3KeysUnchangedSensor(
        task_id="wait_raw_stable",
        aws_conn_id="minio_s3",
        bucket_name=MINIO_BUCKET,
        prefix=RAW_PREFIX.rstrip("/") + "/",
        inactivity_period=15, 
        min_objects=1,
        deferrable=True,      
        soft_fail=False,
        poke_interval=20,
        timeout=60*60*6, 
    )

    list_raw = S3ListOperator(
        task_id="list_raw_keys",
        bucket=MINIO_BUCKET,
        prefix=RAW_PREFIX.rstrip("/") + "/",
        aws_conn_id="minio_s3",
    )

    @task(task_id="choose_keys")
    def choose_keys(ti, dag_run=None) -> List[str]:
        conf = (dag_run.conf if dag_run else {}) or {}
        forced = conf.get("raw_key")
        if forced:
            return [forced]
        keys = ti.xcom_pull(task_ids="list_raw_keys") or []
        keys = [k for k in keys if k.endswith(".jsonl")]
        if not keys:
            raise ValueError("ไม่พบไฟล์ .jsonl ใต้ RAW_PREFIX — โปรดอัปโหลดหรือกำหนด dag_run.conf.raw_key")
        return keys

    # --- NEW (แทรกหลัง choose_keys): กรองเฉพาะไฟล์ที่ 'ยังไม่เคย ingest' ---
    @task(task_id="filter_new")
    def filter_new(keys: List[str]) -> List[str]:
        from psycopg.rows import tuple_row  
        import psycopg

        if not keys:
            return []
        conn = BaseHook.get_connection("pg_app")
        dsn = conn.get_uri().replace("postgres://", "postgresql://")  
        q = """
          SELECT content_ptr->>'object_path' AS k
          FROM documents
          WHERE content_ptr ? 'object_path'
            AND content_ptr->>'bucket' = %s
            AND content_ptr->>'object_path' = ANY(%s)
        """
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(q, (MINIO_BUCKET, keys))
            seen = {row[0] for row in cur.fetchall()}
        new_keys = [k for k in keys if k not in seen]
        return new_keys
    # ---------------------------------------------------------------------------

    # 3) สร้าง env ต่อไฟล์ (เพิ่ม SRC_KEY ลงไป)
    @task(task_id="build_envs")
    def build_envs(keys: List[str]) -> List[Dict[str, str]]:
        base_env = {
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_SECURE": "false",
            "MINIO_BUCKET": "bucket",
            "PG_DSN": "{{ conn.pg_app.get_uri() | replace('postgres://','postgresql://') }}",
        }
        return [{**base_env, "SRC_KEY": k} for k in keys]

    wait_raw_stable >> list_raw  

    keys = choose_keys()                    
    only_new_keys = filter_new(keys)         
    envs = build_envs(only_new_keys)        

    run_pipeline = BashOperator.partial(
        task_id="run_pipeline_all",
        bash_command=(
            "python3 /opt/airflow/dags/run.py "
            "--bucket {{ var.value.MINIO_BUCKET }} "
            "--src-key $SRC_KEY "
            "--text "
            "--dsn {{ conn.pg_app.get_uri() | replace('postgres://','postgresql://') }}"
        ),
        pool="ingest_pool",
    ).expand(env=envs)                         # map ที่ field env จาก build_envs(keys)      # FIX (ของเดิม)
    list_raw >> keys                           # ทำให้ choose_keys รอ list_raw เสมอ         # FIX (ของเดิม)
    @task(task_id="to_label_params")
    def to_label_params(envs: list[dict]) -> list[dict]:
        params = []
        for e in envs:
            params.append({
                "bucket": e["MINIO_BUCKET"],
                "key": e["SRC_KEY"],                # << ส่ง RAW key ตรง ๆ
                "model_id": "SandboxBhh/sentiment-thai-text-model",
                "batch": 8,
            })
        return params
    def _sentiment_shim(bucket: str, key: str, model_id: str, batch: int, **context):
        from types import SimpleNamespace
        from newversion import run_sentiment_and_upsert
        context["dag_run"] = SimpleNamespace(conf={
            "bucket": bucket,
            "key": key,                # รับ RAW key มาก็ยัดให้ตรงนี้
            "model_id": model_id,
            "batch": batch
        })
        return run_sentiment_and_upsert(**context)
    
    label_params = to_label_params(envs)

    sentiment = PythonOperator.partial(
        task_id="sentiment_upsert_mapped",
        python_callable=_sentiment_shim,
        pool="sentiment",
    ).expand(op_kwargs=label_params)
    @task(task_id="to_log_params")
    def to_log_params(envs: list[dict]) -> list[dict]:
        params = []
        for e in envs:
            params.append({
                "bucket": e["MINIO_BUCKET"],
                "key": e["SRC_KEY"],                # << ส่ง RAW key ตรง ๆ
            })
        return params
    def to_log_ingest(bucket: str, key: str, **context):
        from types import SimpleNamespace
        from log import run_log_demo 
        context["dag_run"] = SimpleNamespace(conf={
            "bucket": bucket,
            "key": key,                # รับ RAW key มาก็ยัดให้ตรงนี้
        })
        return run_log_demo (**context)
    
    log_params = to_log_params(envs)

    log_ingest = PythonOperator.partial(
        task_id="log-ingest",
        python_callable= to_log_ingest,
        pool="ingest_pool",
    ).expand(op_kwargs=log_params)
    run_pipeline >> sentiment
    run_pipeline >> log_ingest