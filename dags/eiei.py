# file: /opt/airflow/dags/manual_sentiment_test.py
from __future__ import annotations
import os, json, tempfile
from datetime import datetime
from airflow import DAG
from minio import Minio
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from urllib.parse import urlparse
from minio.error import S3Error
import uuid as _uuid
# ------------------------
def run_sentiment_demo(**context):
    import torch
    import torch.nn.functional as F
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    BUCKET = conf.get("bucket") or "bucket"
    key_raw    =  conf.get("key") or "validated/nectec/bangkokhospital_com/jsonl/bangkokhospital_com.validated.jsonl" or  ""  # required
    if not key_raw:
        raise ValueError("Missing 'key' (validated JSONL object path). Put it in DAG Run -> Conf as {'key': 'validated/.../*.jsonl'}")
    base   = key_raw.replace("raw/", "")
    parent = os.path.dirname(base)
    stem   = os.path.basename(key_raw).replace(".jsonl", "") 
    out_valid_key = f"validated/{parent}/{stem}.validated.jsonl"
    parent_no_jsonl = parent.split("/jsonl", 1)[0]                  # 'nectec/bangkokhospital_com'
    mapping_key      = f"mapping/{parent_no_jsonl}/{stem}.mapped.jsonl"
    client = get_minio_client()
    try:
        client.stat_object(BUCKET, mapping_key)
        KEY = mapping_key
    except S3Error as e:
        print(e)
        if getattr(e, "code", "") in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            KEY = out_valid_key
        else:
            raise
    print(f">>> label from s3://{BUCKET}/{KEY} ...")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    torch.set_grad_enabled(False)
    torch.set_num_threads(max(1, int(os.environ.get("TORCH_NUM_THREADS", "1"))))
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    MODEL_ID = "SandboxBhh/sentiment-thai-text-model"  # 3 คลาส: positive/neutral/negative
    token = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
    model.to(DEVICE)
    model.eval()
    pipe = TextClassificationPipeline(model=model, tokenizer=token, top_k=None, device=0 if DEVICE == "cuda" else -1)
    print("[device] torch.cuda.is_available():", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("[device] torch.version.cuda:", torch.version.cuda)
        print("[device] torch.cuda.get_device_name(0):", torch.cuda.get_device_name(0))

    print("[device] model on:", next(model.parameters()).device) 
    if token.pad_token is None:
        pad_tok = token.eos_token or token.sep_token or token.unk_token
        token.add_special_tokens({"pad_token": pad_tok})
        model.resize_token_embeddings(len(token))
    if getattr(model.config, "pad_token_id", None) is None:
        model.config.pad_token_id = token.pad_token_id

    MODEL_MAX = int(getattr(model.config, "max_position_embeddings", 512))  # e.g. 514 on RoBERTa
    SAFE_LEN  = max(2, min(512, MODEL_MAX) - 2)  # leave 2 slots for <s> and </s>

    def cut_to_safe_len(text: str) -> str:
        enc = token(text, add_special_tokens=True, return_attention_mask=False)
        ids = enc["input_ids"][:SAFE_LEN]              # hard cap BEFORE pipeline
        return token.decode(ids, skip_special_tokens=True)

    def token_count(text: str) -> int:
        enc = token(text, add_special_tokens=True, return_attention_mask=False)
        return len(enc["input_ids"])
    texts: list[str] = []
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
        return key.lstrip('/')  # ห้ามขึ้นต้นด้วย '/'

    def download_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str):
        key = normalize_key(key)          # กัน key ผิดทุกครั้ง
        minio_client.fget_object(bucket, key, local_path)

    def minio_jsonl(bucket: str, key: str):
        client = get_minio_client()
        texts: list[str] = []
        with tempfile.TemporaryDirectory(prefix="test") as tmpdir:
            local_path = os.path.join(tmpdir, "src.jsonl")
            download_minio_object(client,bucket, key, local_path)
            with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    val = obj.get("text") or obj.get("body")
                    if isinstance(val, str) and val.strip():
                        texts.append(val.strip())
        print(f"[manual_sentiment_test] loaded {len(texts)} texts from s3://{bucket}/{key}")
        return texts
    
    texts = minio_jsonl(BUCKET, KEY)
    BATCH = 32
    for i, t in enumerate(texts):
        if token_count(t) > SAFE_LEN:
            texts[i] = cut_to_safe_len(t)
    for i in range(0, len(texts), BATCH):
        batch = texts[i:i+BATCH]
        batch = [cut_to_safe_len(t) if token_count(t) > SAFE_LEN else t for t in batch]
        outputs = pipe(batch,padding=True,truncation=True,max_length=SAFE_LEN,batch_size=BATCH,)
        for j, scores in enumerate(outputs):
            candidates = scores if isinstance(scores, list) else [scores]
            best = max(candidates, key=lambda d: d["score"])
            label_value = best["label"]
            confidence  = round(best["score"] * 100, 2)
            token_cnt   = token_count(batch[j])
            print(f"token_count={token_cnt}, sentiment={label_value}, confidence={confidence}%")
# ------------------------
# DAG 
# ------------------------
with DAG(
    dag_id="eiei_manual_sentiment_test_v2_jsonl",
    description="Manual test of Thai sentiment model from a validated JSONL object",
    schedule=None,                 # Manual only
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["eiei.py","sentiment","manual"],
) as dag:

    run_demo = PythonOperator(
        task_id="run_sentiment_demo",
        python_callable=run_sentiment_demo,
    )

    run_demo
