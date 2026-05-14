import os
import argparse
import subprocess
import sys
import psycopg
import shutil
import shlex
from minio.error import S3Error     
from utils import get_minio_client  # ใช้ตัวเดียวกับโปรเจกต์คุณ

# ----------------------------
# Utils
# ----------------------------

def pick_python() -> str:
    # 1) อนุญาต override ผ่าน ENV
    cand = [
        os.environ.get("PYTHON_BIN"),
        shutil.which("python3"),
        shutil.which("python"),
        "/usr/local/bin/python",
        "/usr/bin/python3",
        "/usr/bin/python",
    ]
    for c in cand:
        if c and os.path.exists(c) and os.access(c, os.X_OK):
            return c
    # สุดท้ายลองคำว่า "python3" ให้ shell หาเอง (ยังดีกว่า string ว่าง)
    return "python3"
# def getname(path: str, dsn: str) -> str | None:
#     p = path.strip("/")
#     parent = p.split(".")[0]
#     parts = parent.split("/")
#     with psycopg.connect(dsn) as conn: 
#         conn.autocommit=True
#         with conn.cursor() as cur:
#             for i in parts[::-1]: 
#                 cur.execute("SELECT id  FROM datasets WHERE name = %s ", (i,))
#                 row = cur.fetchone()
#                 if row:
#                     for i,a in enumerate(row):
#                         return str(a)
#                     break
#     raise RuntimeError(f"dataset id not found from path: {path}")
def getname(path: str, dsn: str) -> str | None:
    if not dsn:
        return None

    p = path.strip("/")
    parent = p.split(".")[0]
    parts = parent.split("/")

    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in parts[::-1]:
                cur.execute(
                    "SELECT id FROM datasets WHERE name = %s LIMIT 1",
                    (name,)
                )
                row = cur.fetchone()
                if row:
                    return str(row[0])

    # ❌ เดิม raise
    # ✅ ใหม่: ปล่อยผ่าน
    print(f"[WARN] dataset not found for path={path} → dataset_id=NULL")
    return None
PY = pick_python()
if not PY or not str(PY).strip():
    raise RuntimeError("Could not resolve a Python interpreter (PYTHON_BIN/python3/python).")
def is_blank(v: str | None) -> bool:
    return v is None or str(v).strip() == "" or str(v).strip().lower() == "none"
def run_and_log(cmd: list[str]):
    print("→", " ".join(shlex.quote(x) for x in cmd))
    r = subprocess.run(cmd, text=True, capture_output=True)  # เก็บ stdout/stderr
    if r.stdout:
        print(r.stdout)   # ดัน stdout เข้า Airflow log
    if r.returncode != 0:
        # สำคัญ: พิมพ์ stderr ด้วย จะได้เห็น traceback เต็ม ๆ
        if r.stderr:
            print(r.stderr, file=sys.stderr)
        raise subprocess.CalledProcessError(r.returncode, cmd)
# ----------------------------
# Pipeline
# ----------------------------
def run_pipeline(
    bucket: str,
    src_key: str,
    limit: int | None = None,
    allowed_status: str = "cleaned",
    min_chars: int = 1,
    text: bool = True,        # default ให้ใส่ --text ตอน ingest
    upsert: bool = True,      # True = upsert, False = ส่ง --no-upsert ไปให้ try.py
    dsn: str | None = None,   # อนุญาตส่ง DSN เข้ามา (ถ้าไม่ส่งจะใช้ default ข้างล่าง)
    dataset_id: str | None = None,  # อนุญาตส่ง dataset_id เข้ามา (ถ้ามี)
):
    # 1) คำนวณเส้นทางปลายทางแบบเป็นระบบ
    base   = src_key.replace("raw/", "")
    parent = os.path.dirname(base)
    stem   = os.path.basename(src_key).replace(".jsonl", "") 
    
    out_clean_key = f"cleaned/{parent}/{stem}.cleaned.jsonl"
    out_clean_log = f"meta/preprocessing_logs/{parent}/{stem}.cleaned.log.jsonl"
    out_valid_log = f"meta/validation_logs/{parent}/{stem}.validated.log.jsonl"
    out_valid_key = f"validated/{parent}/{stem}.validated.jsonl"
    parent_no_jsonl = parent.split("/jsonl", 1)[0]                  # 'nectec/bangkokhospital_com'
    mapping_key      = f"mapping/{parent_no_jsonl}/{stem}.mapped.jsonl"
    #mapping_key   = f"mapping/{parent}/{stem}.mapped.jsonl"
    # DSN ตั้ง default เผื่อเรียกจาก local/airflow แตกต่างกัน
    if not dsn:
        dsn = os.environ.get("PG_DSN", "postgresql://postgres:sukiepal@127.0.0.1:5433/eiei")

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # 2) เช็คไฟล์สคริปต์ให้มีจริง (กัน path เพี้ยน)
    minio_cleaner_py = os.path.join(SCRIPT_DIR, "minio_cleaner.py")
    validate_py      = os.path.join(SCRIPT_DIR, "validate.py")
    mapper_py        = os.path.join(SCRIPT_DIR, "mapper.py")
    try_py           = os.path.join(SCRIPT_DIR, "try.py")
    assert os.path.exists(minio_cleaner_py), f"not found: {minio_cleaner_py}"
    assert os.path.exists(validate_py),      f"not found: {validate_py}"
    assert os.path.exists(mapper_py),        f"not found: {mapper_py}"
    assert os.path.isfile(try_py), f"not a file: {try_py}"

    # 3) CLEAN
    cmd_clean = [
        PY, minio_cleaner_py,
        "--bucket", bucket,
        "--src-key", src_key,
        "--out-clean-key", out_clean_key,
        "--out-logs-key", out_clean_log,
    ]
    if limit is not None:
        cmd_clean += ["--limit", str(limit)]
    print(">>> Running minio_cleaner.py")
    subprocess.run(cmd_clean, check=True)

    # 4) VALIDATE
    cmd_validate = [
        PY, validate_py,
        "--bucket", bucket,
        "--src-cleaned-key", out_clean_key,
        "--out-validated-key", out_valid_key,
        "--out-logs-key", out_valid_log,
        "--allowed-status", allowed_status,
        "--min-chars", str(min_chars),
    ]
    if limit is not None:
        cmd_validate += ["--limit", str(limit)]
    print(">>> Running validate.py")
    subprocess.run(cmd_validate, check=True)

    # 5) MAP
    cmd_mapping = [
        PY, mapper_py,
        "--bucket", bucket,
        "--dataset", stem,
        "--cleaned-key", out_valid_key,
        # ใส่ --csv-key ถ้าต้องแม็ป URL จากไฟล์ mapping CSV ใน MinIO
        # "--csv-key", f"nectec/{root_dataset}/file_url_map.csv",
    ]
    print(">>> Running mapper.py")
    subprocess.run(cmd_mapping, check=True)

    print("✓ Clean/Validate/Map Done")
    print(f"   cleaned:   s3://{bucket}/{out_clean_key}")
    print(f"   cleanlog:  s3://{bucket}/{out_clean_log}")
    print(f"   validated: s3://{bucket}/{out_valid_key}")
    print(f"   validlog:  s3://{bucket}/{out_valid_log}")
    print(f"   mapped:    s3://{bucket}/{mapping_key}")

    # 6) เลือก key สำหรับ ingest:
    #    ถ้ามี mapped ใช้ mapped, ถ้าไม่มี fallback ไป validated
    client = get_minio_client()
    try:
        client.stat_object(bucket_name=bucket, object_name=mapping_key)  
        ingest_key = mapping_key
    except S3Error as e:
        print(e)
        if getattr(e, "code", "") in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            ingest_key = out_valid_key
        else:
            raise
    print(f">>> Ingesting from s3://{bucket}/{ingest_key} ...")
    # 7) INGEST to PostgreSQL (try.py)
    cmd_upload = [
        PY, try_py,
        "--dsn", dsn,
        "--bucket", bucket,
        "--path", ingest_key,
        "--dataset-id", dataset_id or "",
    ]
    if text:
        cmd_upload += ["--text"]
    if not upsert:
        cmd_upload += ["--no-upsert"]

    print(">>> Ingesting to PostgreSQL with try.py ...")
    run_and_log(cmd_upload)
    #subprocess.run(cmd_upload, check=True)
    print("✓ Ingested to PostgreSQL")

# ----------------------------
# CLI
# ----------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "bucket"))
    p.add_argument("--src-key", required=True, help="raw/.../file.jsonl")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--allowed-status", default="cleaned")
    p.add_argument("--min-chars", type=int, default=1)
    p.add_argument("--text", action="store_true", help="เพิ่ม field text ลง DB ขณะ ingest")
    p.add_argument("--dsn", default=os.environ.get("PG_DSN"))  # optional
    dataset_id = None
    if args.dsn:
        dataset_id = getname(args.src_key, args.dsn) #]ลองงงงงงงงงงง
    args = p.parse_args()
    dataset_id = getname(args.src_key,args.dsn)
    bucket = args.bucket
    if is_blank(bucket):
        bucket = os.environ.get("MINIO_BUCKET", "")
    if is_blank(bucket):
        raise SystemExit("ERROR: --bucket ต้องไม่ว่าง (อย่าเป็น 'None'). ตั้งค่า --bucket หรือ ENV MINIO_BUCKET")

    run_pipeline(
        bucket=bucket,
        src_key=args.src_key,
        limit=args.limit,
        allowed_status=args.allowed_status,
        min_chars=args.min_chars,
        text=args.text,
        dsn=args.dsn,
        dataset_id = dataset_id,
    )

if __name__ == "__main__":
    main()
 