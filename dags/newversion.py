# file: /opt/airflow/dags/manual_sentiment_batch_upsert.py
from __future__ import annotations
import os, json, tempfile, hashlib
from datetime import datetime
from urllib.parse import urlparse
from minio.error import S3Error
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from minio import Minio

# ------------------------
# Core task
# ------------------------
def run_sentiment_and_upsert(**context):

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

    # ---- heavy imports (โหลดใน worker) ----
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
    from psycopg import connect
    from psycopg.rows import dict_row

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    # --------- CONFIG (override ได้ผ่าน DAG Run -> Conf) ---------
    BUCKET   = conf.get("bucket")    or "bucket"
    key_raw      = conf.get("key")       or "raw/nectec/bangkokhospital_com/jsonl/bangkokhospital_com.jsonl"
    MODEL_ID = conf.get("model_id")  or "SandboxBhh/sentiment-thai-text-model"
    BATCH    = int(conf.get("batch") or 16)
    base   = key_raw.replace("raw/", "")
    parent = os.path.dirname(base)
    stem   = os.path.basename(key_raw).replace(".jsonl", "") 
    out_valid_key = f"validated/{parent}/{stem}.validated.jsonl"
    parent_no_jsonl = parent.split("/jsonl", 1)[0]                  # 'nectec/bangkokhospital_com'
    mapping_key      = f"mapping/{parent_no_jsonl}/{stem}.mapped.jsonl"
    client = get_minio_client()
    try:
        client.stat_object(bucket_name=BUCKET, object_name= mapping_key)
        KEY = mapping_key
    except S3Error as e:
        print(e)
        if getattr(e, "code", "") in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            KEY = out_valid_key
        else:
            raise
    print(f">>> label from s3://{BUCKET}/{KEY} ...")
    # --------- Connections ---------
    def get_pg_dsn(conn_id="pg_app") -> str:
        c = BaseHook.get_connection(conn_id)
        # แปลง schema postgres:// → postgresql:// เพื่อให้ psycopg v3 ถูกใจ
        return c.get_uri().replace("postgres://", "postgresql://")

    def normalize_key(*parts: str) -> str:
        key = "/".join(
            (p or "").strip().strip("/").replace("\\", "/")
            for p in parts if p and p.strip("/")
        )
        while "//" in key:
            key = key.replace("//", "/")
        return key.lstrip("/")

    def download_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str):
        minio_client.fget_object(bucket_name=bucket, object_name=normalize_key(key),file_path=local_path)

    # --------- JSONL iterator (คืน dict ทีละเรคคอร์ด) ---------
    def iter_jsonl_records(bucket: str, key: str):
        """
        คืน {"text": str, "checksum": str, "raw": obj}
        ถ้าไม่มี checksum ในไฟล์: คำนวณจาก text (อาจไม่เท่ากับที่ ingest ใช้)
        """
        client = get_minio_client()
        with tempfile.TemporaryDirectory(prefix="sentiment") as tmpdir:
            local_path = os.path.join(tmpdir, "src.jsonl")
            download_minio_object(client, bucket, key, local_path)
            with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    text = obj.get("text") or obj.get("body") or obj.get("content")
                    if not (isinstance(text, str) and text.strip()):
                        continue
                    text = text.strip()
                    checksum = obj.get("checksum")
                    if not checksum:
                        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
                    yield {"text": text, "checksum": checksum, "raw": obj}

    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    torch.set_grad_enabled(False)
    torch.set_num_threads(max(1, int(os.environ.get("TORCH_NUM_THREADS", "1"))))

    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    print("[device] cuda:", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("[device] cuda_ver:", torch.version.cuda)
        print("[device] name:", torch.cuda.get_device_name(0))

    token = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
    model.to(DEVICE)
    model.eval()

    pipe = TextClassificationPipeline(
        model=model, tokenizer=token, top_k=None, device=0 if DEVICE == "cuda" else -1
    )

    if token.pad_token is None:
        pad_tok = token.eos_token or token.sep_token or token.unk_token
        token.add_special_tokens({"pad_token": pad_tok})
        model.resize_token_embeddings(len(token))
    if getattr(model.config, "pad_token_id", None) is None:
        model.config.pad_token_id = token.pad_token_id

    MODEL_MAX = int(getattr(model.config, "max_position_embeddings", 512))
    SAFE_LEN  = max(2, min(512, MODEL_MAX) - 2)

    def token_count(text: str) -> int:
        enc = token(text, add_special_tokens=True, return_attention_mask=False)
        return len(enc["input_ids"])

    def cut_to_safe_len(text: str) -> str:
        enc = token(text, add_special_tokens=True, return_attention_mask=False)
        ids = enc["input_ids"][:SAFE_LEN]
        return token.decode(ids, skip_special_tokens=True)

    annotator = f"{MODEL_ID}@v1"

    dsn = get_pg_dsn("pg_app")
    conn = connect(dsn, row_factory=dict_row)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_checksum ON public.documents(checksum)")
        conn.commit()

        total, upserted = 0, 0
        batch_records = []
        for rec in iter_jsonl_records(BUCKET, KEY):
            batch_records.append(rec)
            if len(batch_records) >= BATCH:
                upserted += _process_one_batch(
                    batch_records, pipe, token_count, cut_to_safe_len,
                    SAFE_LEN, MODEL_MAX, annotator, conn
                )
                total += len(batch_records)
                batch_records.clear()

        if batch_records:
            upserted += _process_one_batch(
                batch_records, pipe, token_count, cut_to_safe_len,
                SAFE_LEN, MODEL_MAX, annotator, conn
            )
            total += len(batch_records)

        print(f"[summary] total_texts={total}, upserted={upserted}")
    finally:
        conn.close()


def _process_one_batch(batch_records, pipe, token_count, cut_to_safe_len,
                       SAFE_LEN, MODEL_MAX, annotator, conn):

    texts, checksums = [], []
    for r in batch_records:
        t = r["text"]
        if token_count(t) > SAFE_LEN:
            t = cut_to_safe_len(t)
        texts.append(t)
        checksums.append(r["checksum"])

    outputs = pipe(texts, padding=True, truncation=True, max_length=SAFE_LEN, batch_size=len(texts))

    rows = []
    for i, scores in enumerate(outputs):
        cand = scores if isinstance(scores, list) else [scores]
        best = max(cand, key=lambda d: d["score"])
        rows.append({
            "checksum": checksums[i],
            "label": best["label"],
            "conf": round(best["score"] * 100, 2),
            }
        )

    # batch-lookup document_id
    with conn.cursor() as cur:
        cur.execute("""
            SELECT checksum, id
            FROM public.documents
            WHERE checksum = ANY(%s)
        """, (checksums,))
        doc_map = {r["checksum"]: r["id"] for r in cur.fetchall()}

    insert_rows, miss = [], 0
    for r in rows:
        doc_id = doc_map.get(r["checksum"])
        if not doc_id:
            miss += 1
            continue
        insert_rows.append((
            doc_id, "sentiment", r["label"], r["conf"], annotator
        ))

    if not insert_rows:
        print(f"[batch] size={len(batch_records)} matched=0 miss={miss}")
        return 0

    sql = """
    INSERT INTO public.labels
        (target_id, label_type, label_value, confidence, annotator)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (target_id, label_type)
     DO UPDATE SET
      label_value = EXCLUDED.label_value,
      confidence  = GREATEST(public.labels.confidence, EXCLUDED.confidence),
      created_at  = NOW()
    """
    def _chunks(seq, n=1000):
        for i in range(0, len(seq), n):
            yield seq[i:i+n]
    with conn.cursor() as cur:
        for chunk in _chunks(insert_rows, 1000):
            cur.executemany(sql, chunk)
    conn.commit()

    print(f"[batch] size={len(batch_records)} matched={len(insert_rows)} miss={miss} upserted={len(insert_rows)}")
    return len(insert_rows)


# ------------------------
# DAG
# ------------------------
with DAG(
    dag_id="manual_sentiment_batch_upsert",
    description="Thai sentiment → batch checksum lookup via pg_app → bulk upsert labels",
    schedule=None,               # manual only
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["sentiment","newversion.py", "labels", "batch"],
) as dag:
    run_task = PythonOperator(
        task_id="run_sentiment_and_upsert",
        python_callable=run_sentiment_and_upsert,
    )

    run_task
