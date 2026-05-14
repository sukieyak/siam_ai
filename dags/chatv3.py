import psycopg
import json
import os
import tempfile
from datetime import datetime
from utils import upload_minio_object
from minio import Minio
from urllib.parse import urlparse
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

def run_chat_log(**context):

    
    def get_minio_client(conn_id="minio_s3") -> Minio:
        c = BaseHook.get_connection(conn_id)
        ep = (c.extra_dejson.get("endpoint_url")
              or f"{c.schema or 'http'}://{c.host}:{c.port or 9000}")
        u = urlparse(ep)
        return Minio(
            endpoint=f"{u.hostname}:{u.port or (443 if u.scheme=='https' else 80)}",
            access_key=c.login,
            secret_key=c.password,
            secure=(u.scheme == "https"),
        )
    run_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"{run_date}.jsonl"
    conn = BaseHook.get_connection("pg_chat")
    dsn = conn.get_uri()

    client = get_minio_client()
    def normalize_ts(ts):
        if ts >= 1_000_000_000_000: 
            ts = ts // 1000
        return ts
    minio_bucket = "chat"
    minio_path = f"eiei/{filename}"

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, chat, created_at, updated_at
                FROM chat
            """)
            rows = cur.fetchall()

    with tempfile.TemporaryDirectory() as tmpdir:
        filename = os.path.join(tmpdir, "chat.jsonl")

        with open(filename, "w", encoding="utf-8") as f:
            for row in rows:
                chat = json.loads(row[2])
                messages = chat.get("messages", [])

                created = datetime.fromtimestamp(normalize_ts(row[3])).isoformat()
                updated = datetime.fromtimestamp(normalize_ts(row[4])).isoformat()

                clean_messages = []

                for msg in messages:
                    role = msg.get("role")
                    content = msg.get("content")

                    clean_msg = {
                        "role": role,
                        "content": content
                    }

                    if role == "assistant":
                        clean_msg["model"] = msg.get("modelName", "assistant")

                    clean_messages.append(clean_msg)

                jsonl_obj = {
                    "chat_id": row[0],
                    "title": row[1],
                    "messages": clean_messages,
                    "created_at": created,
                    "updated_at": updated,
                }

                f.write(json.dumps(jsonl_obj, ensure_ascii=False) + "\n")

        upload_minio_object(
            client,
            bucket=minio_bucket,
            key=minio_path,
            local_path=filename
        )

    print(f"({len(rows)} lines) & uploaded")
with DAG(
    dag_id="Chat_log",
    description="Export chat logs to MinIO",
    schedule="@weekly",            
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=[],
) as dag:
    run_task = PythonOperator(
        task_id="run_chat_log",
        python_callable=run_chat_log,
    )

    run_task