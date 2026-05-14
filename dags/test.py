from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
import json
import io


BUCKET = "test"
SRC_KEY = "raw/BEST-Article.cleaned.jsonl"
DEST_KEY = "stream-output/cleaned_test.jsonl"

PART_SIZE = 5 * 1024 * 1024  # 5MB


def streaming_process_and_upload(**context):
    hook = S3Hook(aws_conn_id="minio_s3")
    client = hook.get_conn()

    response = client.get_object(Bucket=BUCKET, Key=SRC_KEY)
    body = response["Body"]


    mp = client.create_multipart_upload(Bucket=BUCKET, Key=DEST_KEY)
    upload_id = mp["UploadId"]

    parts = []
    buffer = io.BytesIO()
    part_number = 1
    total_lines = 0

    try:
        for line in body.iter_lines():
            if not line:
                continue

            obj = json.loads(line)

            # 🔥 ตัวอย่าง process เล็ก ๆ
            text = obj.get("text", "")
            obj["text"] = text.upper()  # แปลงเป็น uppercase

            new_line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
            buffer.write(new_line)
            total_lines += 1

            # ถ้า buffer เกิน PART_SIZE → upload part
            if buffer.tell() >= PART_SIZE:
                buffer.seek(0)

                part = client.upload_part(
                    Bucket=BUCKET,
                    Key=DEST_KEY,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer.read(),
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": part["ETag"],
                })

                print(f"Uploaded part {part_number}")
                part_number += 1
                buffer = io.BytesIO()

        # -------------------------
        # 3️⃣ Upload ส่วนสุดท้าย
        # -------------------------
        if buffer.tell() > 0:
            buffer.seek(0)
            part = client.upload_part(
                Bucket=BUCKET,
                Key=DEST_KEY,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer.read(),
            )

            parts.append({
                "PartNumber": part_number,
                "ETag": part["ETag"],
            })

        # -------------------------
        # 4️⃣ Complete multipart
        # -------------------------
        client.complete_multipart_upload(
            Bucket=BUCKET,
            Key=DEST_KEY,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        print(f"Successfully processed: {total_lines} lines")

    except Exception as e:
        print("Error occurred. Aborting multipart upload.")
        client.abort_multipart_upload(
            Bucket=BUCKET,
            Key=DEST_KEY,
            UploadId=upload_id,
        )
        raise e


with DAG(
    dag_id="minio_streaming_multipart_onefile",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["streaming", "multipart", "jsonl"],
) as dag:

    run_stream = PythonOperator(
        task_id="stream_process_and_upload",
        python_callable=streaming_process_and_upload,
    )

    run_stream