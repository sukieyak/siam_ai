import os, csv, json, sys, tempfile, argparse, uuid
from minio.error import S3Error 
from utils import (get_minio_client,download_minio_object,upload_minio_object,now_thai,)
run_at = now_thai()
def normalize_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    base = os.path.basename(k)
    return base or k

def load_file_url_map(csv_path: str, flog) -> dict:
    """อ่าน file_url_map.csv → map[filename] = url  (ข้าม header แปลก/มี BOM ได้)"""
    mapping = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fn_keys = [k for k in reader.fieldnames or [] if k.lower() == "filename"]
        url_keys = [k for k in reader.fieldnames or [] if k.lower() == "url"]
        if not fn_keys or not url_keys:
            flog.write(json.dumps({
                "log_id": str(uuid.uuid4()),
                "step": "load_csv",
                "status": "failed",
                "details": {"reason": "missing_headers", "headers": reader.fieldnames},
                "executed_at": run_at,
                "executed_by": "mapper"
            }, ensure_ascii=False) + "\n")
            return mapping
        fn_key, url_key = fn_keys[0], url_keys[0]

        dups = set()
        for row in reader:
            fn = normalize_key(row.get(fn_key, ""))
            url = (row.get(url_key) or "").strip()
            if not fn or not url:
                continue
            if fn in mapping:
                dups.add(fn)
            mapping[fn] = url

        for k in sorted(dups):
            flog.write(json.dumps({
                "log_id": str(uuid.uuid4()),
                "step": "load_csv",
                "status": "warn",
                "details": f"duplicate_in_csv:{k}",
                "executed_at": run_at,
                "executed_by": "mapper"
            }, ensure_ascii=False) + "\n")
    return mapping

def merge_url_into_obj(obj: dict, url: str, prefer_csv: bool, flog):
    meta = obj.get("metadata")
    if meta is None or not isinstance(meta, dict):
        meta = {}
    if prefer_csv or ("url" not in meta) or meta.get("url") in (None, "", {}):
        meta["url"] = url
        obj["metadata"] = meta
        flog.write(json.dumps({
            "log_id": str(uuid.uuid4()),
            "target_id": obj.get("id"),
            "step": "map",
            "status": "mapped",
            "details": normalize_key(obj.get("filename") or obj.get("doc_id") or obj.get("source_id") or ""),
            "executed_at": run_at,
            "executed_by": "mapper"
        }, ensure_ascii=False) + "\n")
    return obj

def run_mapping(in_cleaned_path: str, csv_path: str, out_jsonl_path: str, out_logs_path: str, prefer_csv: bool):
    n_in = n_out = n_mapped = n_unmapped = 0
    with open(out_logs_path, "w", encoding="utf-8") as flog:
        url_map = load_file_url_map(csv_path, flog)

        with open(in_cleaned_path, "r", encoding="utf-8") as fi, \
             open(out_jsonl_path, "w", encoding="utf-8") as fo:
            for line in fi:
                if not line.strip():
                    continue
                n_in += 1
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    flog.write(json.dumps({
                        "log_id": str(uuid.uuid4()),
                        "step": "read_jsonl",
                        "status": "failed",
                        "details": f"json_error:{str(e)}",
                        "executed_at": run_at,
                        "executed_by": "mapper"
                    }, ensure_ascii=False) + "\n")
                    continue

                key = normalize_key(obj.get("filename") or obj.get("doc_id") or obj.get("source_id") or "")
                if key and key in url_map:
                    obj = merge_url_into_obj(obj, url_map[key], prefer_csv, flog)
                    n_mapped += 1
                else:
                    flog.write(json.dumps({
                        "log_id": str(uuid.uuid4()),
                        "target_id": obj.get("id"),
                        "step": "map",
                        "status": "unmapped",
                        "details": key or None,
                        "executed_at": run_at,
                        "executed_by": "mapper"
                    }, ensure_ascii=False) + "\n")
                    n_unmapped += 1

                fo.write(json.dumps(obj, ensure_ascii=False) + "\n")
                n_out += 1


    return  {"n_in": n_in,"n_out": n_out,"n_mapped": n_mapped,"n_unmapped": n_unmapped,}
def main():
    ap = argparse.ArgumentParser(description="Map file_url_map.csv (filename→url) into CLEANED JSONL on MinIO (auto-detect paths by dataset).")
    ap.add_argument("--bucket", default="ai-datasets", help="MinIO bucket (default: ai-datasets)")
    ap.add_argument("--dataset", required=True, help="เช่น bangkokhospital_com, bot_or_th, chillwithmeblog_com")
    ap.add_argument("--prefer-csv", action="store_true", help="ให้ค่าจาก CSV ทับค่า url ใน metadata ถ้ามี")
    ap.add_argument("--csv-key", help="override: raw/nectec/{dataset}/file_url_map.csv")
    ap.add_argument("--cleaned-key", help="override: sukie/nectec/{dataset}/{dataset}.cleaned.jsonl")
    ap.add_argument("--out-mapped-key", help="override: sukie/nectec/{dataset}/{dataset}.mapped.jsonl")
    ap.add_argument("--out-logs-key", help="override: meta/mapping_logs/{dataset}.map.log.jsonl")
    args = ap.parse_args()

    dataset = args.dataset.strip().rstrip("/")
    #csv_key       = args.csv_key        or f"raw/nectec/{dataset}/file_url_map.csv"
    csv_key        = args.csv_key        or f"raw/nectec/{dataset}/file_url_map.csv"##for local only 
    cleaned_key    = args.cleaned_key    or f"validated/nectec/{dataset}/jsonl/{dataset}.validated.jsonl"
    out_mapped_key = args.out_mapped_key or f"mapping/nectec/{dataset}/{dataset}.mapped.jsonl"
    out_logs_key   = args.out_logs_key   or f"meta/mapping_logs/{dataset}/{dataset}.map.log.jsonl"
    client = get_minio_client()
    try:
        client.stat_object(    bucket_name=args.bucket,object_name=csv_key,)
    except S3Error as e:
        if e.code == "NoSuchKey":
            print(f"↷ Skip mapping: CSV not found at s3://{args.bucket}/{csv_key}")
            return 0  # [EDIT] ออกจาก main() แบบสำเร็จ (ไม่ถือเป็น error)
        else:
            raise  # error อื่นๆ ให้แสดงตามปกติ
    with tempfile.TemporaryDirectory(prefix="mapper_minio_") as td:
        local_csv     = os.path.join(td, "file_url_map.csv")
        local_cleaned = os.path.join(td, "cleaned.jsonl")
        local_mapped  = os.path.join(td, "mapped.jsonl")
        local_logs    = os.path.join(td, "map_logs.jsonl")

        print(f"[1/4] Download CSV     → s3://{args.bucket}/{csv_key}")
        download_minio_object(client, args.bucket, csv_key,     local_csv)

        print(f"[2/4] Download CLEANED → s3://{args.bucket}/{cleaned_key}")
        download_minio_object(client, args.bucket, cleaned_key, local_cleaned)

        print(f"[3/4] Mapping (prefer_csv={args.prefer_csv}) …")
        run_mapping(local_cleaned, local_csv, local_mapped, local_logs, args.prefer_csv)
        stats = run_mapping(local_cleaned, local_csv, local_mapped, local_logs, args.prefer_csv)

        print(f"  \t สรุป: in={stats['n_in']}, out={stats['n_out']}, mapped={stats['n_mapped']}, unmapped={stats['n_unmapped']}")

        print(f"[4/4] Upload outputs:")
        print(f"      mapped → s3://{args.bucket}/{out_mapped_key}")
        upload_minio_object(client, args.bucket, out_mapped_key, local_mapped, content_type="application/json")
        print(f"      logs   → s3://{args.bucket}/{out_logs_key}")
        upload_minio_object(client, args.bucket, out_logs_key,  local_logs,   content_type="application/json")
        print("✓ done")
if __name__ == "__main__":
    sys.exit(main() or 0) 
