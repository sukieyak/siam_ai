from __future__ import annotations
import os, json, tempfile
from datetime import datetime
from airflow import DAG
import sys
import traceback
import psycopg
from psycopg.types.json import Json
from psycopg.rows import dict_row
from minio import Minio
import uuid as _uuid
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from urllib.parse import urlparse
from minio.error import S3Error

def run_log_demo(**context):
    def get_minio_client(conn_id="minio_s3"):
        conn = BaseHook.get_connection(conn_id)
        ep = (conn.extra_dejson.get("endpoint_url")
            or f"{conn.schema or 'http'}://{conn.host}:{conn.port or 9000}")
        u = urlparse(ep)
        client = Minio(
            endpoint=f"{u.hostname}:{u.port or (443 if u.scheme=='https' else 80)}",
            access_key=conn.login,
            secret_key=conn.password,
            secure=(u.scheme == "https"),
        )
        return client
    def normalize_key(*parts: str) -> str:
        key = "/".join(
            p.strip().strip("/").replace("\\", "/")
            for p in parts if p and p.strip("/")
        )
        while '//' in key:
            key = key.replace('//','/')
        return key.lstrip('/')

    def download_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str):
        key = normalize_key(key)         
        minio_client.fget_object( bucket_name=bucket,object_name= key,file_path= local_path)
    def get_pg_dsn(conn_id="pg_app") -> str:
        c = BaseHook.get_connection(conn_id)
        return c.get_uri().replace("postgres://", "postgresql://")
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    BUCKET = conf.get("bucket") or "bucket"
    key_raw    =  conf.get("key") or "raw/nectec/bangkokhospital_com/jsonl/bangkokhospital_com.jsonl"
    base   = key_raw.replace("raw/", "")
    parent = os.path.dirname(base)
    stem   = os.path.basename(key_raw).replace(".jsonl", "") 
    mapping_log = f"meta/mapping_logs/{stem}/{stem}.map.log.jsonl"
    #meta/mapping_logs/bangkokhospital_com/bangkokhospital_com.map.log.jsonl
    preprocess_log = f"meta/preprocessing_logs/{parent}/{stem}.cleaned.log.jsonl"
    validate_log = f"meta/validation_logs/{parent}/{stem}.validated.log.jsonl"
    client = get_minio_client()
    
#/bucket/meta/mapping_logs/nectec/bangkokhospital_com/bangkokhospital_com.map.log.jsonl
#/ bucket/meta/mapping_logs/bangkokhospital_com/bangkokhospital_com.map.log.jsonl
    def ingest_log_to_db(BUCKET: str, KEY: str):  
        dsn = get_pg_dsn("pg_app")
        with tempfile.TemporaryDirectory(prefix="log") as tmpdir:
                local_path = os.path.join(tmpdir, "src.jsonl")
                print(f" Download: s3://{BUCKET}/{KEY} -> {local_path}")
                download_minio_object(client, BUCKET, KEY, local_path)
                dsn = get_pg_dsn("pg_app")
            
                
                with psycopg.connect(dsn) as conn:
                        conn.autocommit = True
                        with conn.cursor(row_factory=dict_row) as cur, open(local_path, "r", encoding="utf-8") as fi:
                            for i, line in enumerate(fi, start=1):
                                line = line.strip()
                                if not line:
                                    continue

                                try:
                                    obj = json.loads(line)
                                except json.JSONDecodeError:
                                    print(f"skip line {i}: JSON decode error")
                                    continue

                                id          =  obj.get("log_id")  
                                document_id = obj.get("target_id")
                                step        = obj.get("step") 
                                status      = obj.get("status")
                                details     = obj.get("details")
                                executed_by = obj.get("executed_by")  
                                executed_at = obj.get("executed_at", 0)

                                sql = """
                                INSERT INTO preprocess_logs
                                ("id","document_id","step","status","details","executed_by","executed_at")
                                VALUES (%(id)s,%(document_id)s,%(step)s,%(status)s,%(details)s,%(executed_by)s,%(executed_at)s)
                                """
                                sql += """
                                    ON CONFLICT (id)
                                        DO UPDATE SET
                                            status = EXCLUDED.status,
                                            details = EXCLUDED.details,
                                            executed_at = EXCLUDED.executed_at
                                    """
                                
                                try:
                                    cur.execute(sql, {
                                    "id": id,
                                    "document_id": document_id,
                                    "step": step,
                                    "status": status,
                                    "details": details,
                                    "executed_by": executed_by,
                                    "executed_at": executed_at,
                                })
                                except psycopg.Error as e:
                                    print("\n--- DB ERROR ---")
                                    print("type:", type(e).__name__)
                                    print("msg:", str(e))
             
    ingest_log_to_db(BUCKET, preprocess_log)
    ingest_log_to_db(BUCKET, validate_log)
    try:
        client.stat_object(bucket_name=BUCKET, object_name=mapping_log)
        ingest_log_to_db(BUCKET, mapping_log)
    except S3Error as e:
        print(f"skip mapping log:{e}")

# ------------------------
# DAG 
# ------------------------
with DAG(
    dag_id="log_ingest_manual",
    description="Manual test of Thai sentiment model from a validated JSONL object",
    schedule=None,                 # Manual only
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["log.py","manual"],
) as dag:

    run_demo = PythonOperator(
        task_id="run_log_ingest",
        python_callable=run_log_demo,
    )

    run_demo
