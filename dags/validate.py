import os, json,sys, argparse, tempfile, re, uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from utils import (get_minio_client,download_minio_object,upload_minio_object,checksum_sha256,now_thai)

REQUIRED_FIELDS = ["filename", "source", "text", "checksum", "status", "processed_at"]
ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")
def _is_valid_iso_ts(ts: str) -> bool:
    if not isinstance(ts, str):
        return False
    s = ts.strip()

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        datetime.fromisoformat(s)  
        return True
    except ValueError:
        return False

def validate_record(obj: dict,
                    i: int,
                    seen_checksums: set,
                    allowed_input_status: set,
                    min_chars: int = 1):

    if not isinstance(obj, dict):
        return False, "type:not_object", None

    for f in REQUIRED_FIELDS:
        if f not in obj:
            return False, f"missing_field:{f}", None

    filename = obj.get("filename")
    source   = obj.get("source")
    text     = obj.get("text")
    checksum = obj.get("checksum")
    status   = obj.get("status")
    ts       = obj.get("processed_at")

    if not isinstance(filename, str) or not filename.strip():
        return False, "bad_filename", None
    if not isinstance(source, str) or not source.strip():
        return False, "bad_source", None
    if not isinstance(text, str) or len(text) < min_chars:
        return False, f"bad_text_len:{len(text) if isinstance(text,str) else 'NA'}", None
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        return False, "bad_checksum_format", None
    if not isinstance(status, str) or status not in allowed_input_status:
        return False, f"bad_status:{status}", None
    if not _is_valid_iso_ts(ts):
        return False, "bad_processed_at_format", None
    wc = obj.get("word_count")
    cc = obj.get("char_count")
    if wc is not None and (not isinstance(wc, int) or wc < 0):
        return False, "bad_word_count", None
    if cc is not None and (not isinstance(cc, int) or cc < 0):
        return False, "bad_char_count", None
    if cc is not None and isinstance(text, str) and cc != len(text):
        return False, f"char_count_mismatch:{cc}!={len(text)}", None

    if isinstance(text, str):
        calc = checksum_sha256(text)
        if calc != checksum:
            return False, "checksum_mismatch", None

    if checksum in seen_checksums:
        return False, "dup_checksum_in_file", None

    normalized = dict(obj)
    normalized["status"] = "validated"
    return True, "", normalized

def main():
    ap = argparse.ArgumentParser(description="Validate cleaned JSONL before mapping (standalone, no utils.py)")
    ap.add_argument("--bucket", required=True, help="MinIO bucket เช่น ai-datasets")
    ap.add_argument("--src-cleaned-key", required=True, help="object key cleaned.jsonl ต้นทาง")
    ap.add_argument("--out-validated-key", required=True, help="object key ปลายทาง validated.jsonl")
    ap.add_argument("--out-logs-key", required=True, help="object key ปลายทาง validation_logs.jsonl")
    ap.add_argument("--min-chars", type=int, default=1, help="ขั้นต่ำตัวอักษรของ text (กันเคสว่าง)")
    ap.add_argument("--allowed-status", default="cleaned", help="สถานะ input ที่อนุญาต (คั่นด้วยคอมมา) เช่น 'cleaned,reviewed'")
    ap.add_argument("--limit", type=int, default=None, help="ประมวลผลเฉพาะ N บรรทัดแรก (สำหรับทดสอบ)")
    args = ap.parse_args()

    allowed_input_status = set(s.strip() for s in args.allowed_status.split(",") if s.strip())

    client = get_minio_client()

    with tempfile.TemporaryDirectory(prefix="validate_") as tmpdir:
        src_local = os.path.join(tmpdir, "cleaned.jsonl")
        out_valid_local = os.path.join(tmpdir, "validated.jsonl")
        out_logs_local  = os.path.join(tmpdir, "validation_logs.jsonl")

        print(f"[1/4] ดาวน์โหลด cleaned → s3://{args.bucket}/{args.src_cleaned_key}")
        download_minio_object(client, args.bucket, args.src_cleaned_key, src_local)

        n_in = n_ok = n_bad = 0
        seen_checksums = set()
        #now_iso = datetime.utcnow().isoformat() + "Z"
        

        print(f"[2/4] เริ่ม Validate (limit={args.limit}, allowed_status={allowed_input_status})")
        with open(src_local, "r", encoding="utf-8") as fi, \
             open(out_valid_local, "w", encoding="utf-8") as fvalid, \
             open(out_logs_local, "w", encoding="utf-8") as flog:

            for i, line in enumerate(fi):
                if args.limit and i >= args.limit:
                    break
                n_in += 1
                
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    flog.write(json.dumps({
                        "log_id": str(uuid.uuid4()),
                        "target_filename": None,
                        "step": "read_json",
                        "status": "failed",
                        "details": f"json_error:{str(e)}",
                        "executed_by": "validator",
                        "executed_at": now_thai()
                    }, ensure_ascii=False) + "\n")
                    n_bad += 1
                    continue

                ok, reason, normalized = validate_record(
                    obj=obj,
                    i=i,
                    seen_checksums=seen_checksums,
                    allowed_input_status=allowed_input_status,
                    min_chars=args.min_chars
                )

                if ok:
                    seen_checksums.add(normalized["checksum"])
                    fvalid.write(json.dumps(normalized, ensure_ascii=False) + "\n")
                    n_ok += 1
                    flog.write(json.dumps({
                        "log_id": str(uuid.uuid4()),
                        "target_id": obj.get("id"),
                        "step": "validate",
                        "status": "success",
                        "details": "",
                        "executed_by": "validator",
                        "executed_at": now_thai()
                    }, ensure_ascii=False) + "\n")
                else:
                    n_bad += 1
                    flog.write(json.dumps({
                        "log_id": str(uuid.uuid4()),
                        "target_id": obj.get("id"),
                        "step": "validate",
                        "status": "failed",
                        "details": reason,
                        "executed_by": "validator",
                        "executed_at": now_thai()
                    }, ensure_ascii=False) + "\n")

        print(f"        สรุป: in={n_in}, validated={n_ok}, invalid={n_bad}")

        print(f"[3/4] Uploading validated → s3://{args.bucket}/{args.out_validated_key}")
        upload_minio_object(client, args.bucket, args.out_validated_key, out_valid_local)

        print(f"[4/4] Upoading logs → s3://{args.bucket}/{args.out_logs_key}")
        upload_minio_object(client, args.bucket, args.out_logs_key, out_logs_local)

        print("✓ เสร็จสิ้น")

if __name__ == "__main__":
    sys.exit(main() or 0)
