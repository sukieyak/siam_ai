import os,sys, re, argparse, tempfile, unicodedata, uuid
from ast import literal_eval
from time import perf_counter
from bs4 import BeautifulSoup   
from pythainlp.tokenize import word_tokenize
from utils import (get_minio_client,download_minio_object,upload_minio_object,checksum_sha256,now_thai,thai_ratio,)
from urllib.parse import urlsplit, urlunsplit 
from langdetect import detect, DetectorFactory
import json

URL_RE = re.compile(r'(https?://[^\s)>\]"}]+)', re.IGNORECASE) 
TRAILING_PUNCT = '.,);]}>\"\'!?、。’”'                                     
run_at = now_thai()
def _strip_trailing_punct(u: str) -> str:                                 
    while u and u[-1] in TRAILING_PUNCT: u = u[:-1]
    return u

def _normalize_url(u: str) -> str:                                       
    u = (u or "").strip()
    if not u: return u
    u = _strip_trailing_punct(u)
    try:
        p = urlsplit(u)
        return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path, p.query, ""))
    except Exception:
        return u

def extract_urls(text: str) -> list[str]:                                 
    raw = [m.group(1) for m in URL_RE.finditer(text or "")]
    seen, out = set(), []
    for r in raw:
        n = _normalize_url(r)
        if n and n not in seen:
            seen.add(n); out.append(n)
    return out

DetectorFactory.seed = 0  
THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style)\b[^>]*>.*?</\1\s*>")
_ESCAPE_MAP = [(r"\\r\\n", "\n"), (r"\\n",    "\n"), (r"\\t",    "\t"), (r"\\u00A0","\u00A0"), (r"\\u200B","\u200B"), (r"\\u200C","\u200C"),    (r"\\u200D","\u200D"),   
]
def detect_lang(text: str) -> str:
    if not text or not text.strip():
        return ("und")
    try:
        code = detect(text)
        return code 
    except Exception:
        return ("und")

def unescape_backslashes(text: str) -> str:
    if not text:
        return text
    t = text
    for pat, repl in _ESCAPE_MAP:
        t = re.sub(pat, repl, t)
    return t

def normalize_unicode(text: str) -> str:
    if text is None:
        return ""
    t = unicodedata.normalize("NFC", text)
    t = t.replace("\u00A0", " ")
    t = t.replace("\u200B", "").replace("\u200C", "").replace("\u200D", "")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    return t

def normalize_whitespace(text: str) -> str:
    t = re.sub(r"[ \t]+", " ", text)
    t = re.sub(r"[ \t]+\n", " \n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def clean_text_pipeline(text: str):
    logs = []
    t0 = text or ""
    t0u = unescape_backslashes(t0)
    if t0u != t0:
        logs.append(("unescape_backslashes", "applied"))
    t1 = normalize_unicode(t0u)
    if t1 != t0u:
        logs.append(("normalize_unicode", "applied"))
    t2 = SCRIPT_STYLE_RE.sub("", t1)
    t2 = BeautifulSoup(t2, "lxml").get_text()
    if "\u00A0" in t2:
        t2 = t2.replace("\u00A0", " ")
        logs.append(("post_soup_nbsp", "replaced_nbsp"))
    logs.append(("remove_html", "applied"))
    t3 = normalize_whitespace(t2)
    if t3 != t2:
        logs.append(("normalize_whitespace", "collapsed"))
    else:
        logs.append(("normalize_whitespace", "no_change"))

    return t3, logs

def process_jsonl_file(in_path: str, out_clean_path: str, out_logs_path: str, min_chars: int,
                        min_thai_ratio: float, limit: int | None , bucket: str = None, object_path: str = None,source_name: str = None):
    seen_hashes = set()

    n_in = n_clean = n_failed = 0
    start = perf_counter()
    with (
        open(in_path, "r", encoding="utf-8") as fi, 
        open(out_clean_path, "w", encoding="utf-8") as fo, 
        open(out_logs_path, "w", encoding="utf-8") as flog
    ):
        for i, line in enumerate(fi):
            if limit and i >= limit:
                break
            n_in += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                flog.write(json.dumps({
                    "log_id": str(uuid.uuid4()),
                    "target_id": None,
                    "step": "validate_input",
                    "status": "failed",
                    "details": f"json_error:{str(e)}",
                    "executed_by": "minio_cleaner",
                    "executed_at": run_at
                }, ensure_ascii=False) + "\n")
                n_failed += 1
                continue

            source = (obj.get("source") or "").strip() or source_name

            text = obj.get("text", "") or ""
            filename = obj.get("filename")
            if not filename:
                sid = obj.get("source_id")
                if sid and str(sid).lower() != "none":
                    filename = str(sid)
                else:
                    filename = f"{source}_{i+1:07d}.txt" 
           

            document_id = str(uuid.uuid4())

            flog.write(json.dumps({
                "log_id": str(uuid.uuid4()),
                "target_id": document_id,
                "step": "validate_input",
                "status": "success" if isinstance(text, str) else "failed",
                "details": json.dumps({"original_chars": len(text)}),
                "executed_by": "minio_cleaner",
                "executed_at": run_at
            }, ensure_ascii=False) + "\n")

            clean_text, steps = clean_text_pipeline(text)
            for st_name, st_detail in steps:
                flog.write(json.dumps({
                    "log_id": str(uuid.uuid4()),
                    "target_id": document_id,
                    "step": st_name,
                    "status": "success",
                    "details": st_detail,
                    "executed_by": "minio_cleaner",
                    "executed_at": run_at
                }, ensure_ascii=False) + "\n")

            th_ratio = thai_ratio(clean_text)
            if th_ratio >= 0.90:
                language  = "th"
            else:
                language = detect_lang(clean_text)
            chksum = checksum_sha256(clean_text)
            status = "cleaned"
            reason = ""
            if len(clean_text) < min_chars:
                status = "failed"; reason = f"too_short:{len(clean_text)}"
            elif th_ratio < min_thai_ratio:
                status = "failed"; reason = f"thai_ratio_low:{th_ratio:.2f}"
            elif chksum in seen_hashes:
                status = "failed"; reason = "dup_exact"
            else:
                seen_hashes.add(chksum)
            flog.write(json.dumps({
                "log_id": str(uuid.uuid4()),
                "target_id": document_id,
                "step": "finalize_cleaned",
                "status": "success" if status == "cleaned" else "failed",
                "details": json.dumps({"char_count": len(clean_text), "thai_ratio": th_ratio, "reason": reason}),
                "executed_by": "minio_cleaner",
                "executed_at": run_at
            }, ensure_ascii=False) + "\n")
            metadata = {}
            content_ptr = {}
            content_ptr = {
                    "bucket": bucket or "",
                    "object_path": object_path or "",
                    "line_no": i+1,
                }
            # รวมข้อมูล metadata เดิม (ถ้ามี) โดยพยายามแปลง

            raw_meta = obj.get("meta")
            MISSING_SENTINELS = {"", "none", "null", "nan"}
            if isinstance(raw_meta, str):
                s = raw_meta.strip()
                if s.lower() in MISSING_SENTINELS:
                    raw_meta = None
            if raw_meta:
                if isinstance(raw_meta, dict):
                    metadata.update(raw_meta)
                elif isinstance(raw_meta, str):
                    parsed_ok = False
                    try:
                        parsed = json.loads(raw_meta)
                        if isinstance(parsed, dict):
                            metadata.update(parsed)
                            parsed_ok = True
                    except json.JSONDecodeError:
                        pass
                    if not parsed_ok:
                        try:
                            parsed = literal_eval(raw_meta)
                            if isinstance(parsed, dict):
                                metadata.update(parsed)
                                parsed_ok = True
                        except (ValueError, SyntaxError):
                            parsed_ok = False
                    if not parsed_ok and raw_meta.strip():
                        metadata["raw_meta"] = raw_meta
            if obj.get("created_date"):
                metadata["created_date"] = obj["created_date"]
            if obj.get("updated_date"):
                metadata["updated_date"] = obj["updated_date"]
    ##Url tools begin ##                              
            links = extract_urls(text)  # ดึงลิงก์ทั้งหมดจากข้อความ
            prev_links = metadata["links_all"] if isinstance(metadata.get("links_all"), list) else []
            merged, seen = [], set()
            for u in prev_links + links:
                if u and u not in seen:
                    seen.add(u); merged.append(u)
            if merged:
                metadata["links_all"] = merged

    ## Url tools end ##
            not_for_you_boi = {"bucket", "object_path", "line_no"}
            for k in list(metadata.keys()):
                if k in not_for_you_boi:
                    metadata.pop(k, None)
            fo.write(json.dumps(
                {
                "id": document_id,
                "filename": filename,
                "source": source,  
                "status": status,
                # "bucket": bucket,              
                # "object_path": object_path,     
                # "line_no": i+1,
                "content_ptr": content_ptr,
                "word_count": len(word_tokenize(clean_text, engine="newmm")),
                "char_count": len(clean_text),
                "thai_ratio": round(th_ratio, 3),
                "language": language,
                ##"language_score": lang_score,
                "metadata": metadata,
                "processed_at": run_at,
                "checksum": chksum,
                "text": clean_text
            }, ensure_ascii=False) + "\n")
            

            if status == "cleaned":
                n_clean += 1
            else:
                n_failed += 1
    end = perf_counter() - start
    return {"in": n_in, "cleaned": n_clean, "failed": n_failed, "duration" : end}
def main():
    ap = argparse.ArgumentParser(description="Clean JSONL on MinIO and upload results")
    ap.add_argument("--bucket", required=True, help="ชื่อ bucket บน MinIO เช่น ai-datasets")
    ap.add_argument("--src-key", required=True, help="object key ต้นทาง เช่น raw/nectec/.../file.jsonl")
    ap.add_argument("--out-clean-key", required=True, help="object key ปลายทางของไฟล์ cleaned.jsonl")
    ap.add_argument("--out-logs-key", required=True, help="object key ปลายทางของไฟล์ preprocessing_logs.jsonl")
    ap.add_argument("--min-chars", type=int, default=10, help="จำนวนcharacterขั้นต่ำ ถ้าไม่ใส่ deafualt คือ 10 ตัว")
    ap.add_argument("--min-thai-ratio", type=float, default=0.1, help="สัดส่วนตัวอักษรไทยขั้นต่ำ")
    ap.add_argument("--limit", type=int, default=None, help="ประมวลผลเฉพาะ N บรรทัดแรก")
    args = ap.parse_args()

    client = get_minio_client()
    sum_time = perf_counter()
    with tempfile.TemporaryDirectory(prefix="minio_clean") as tmpdir:
        src_local = os.path.join(tmpdir, "src.jsonl")
        out_clean_local = os.path.join(tmpdir, "cleaned.jsonl")
        out_logs_local  = os.path.join(tmpdir, "preprocessing_logs.jsonl")

        print(f"[1/4] Dowloading File from MinIO: s3://{args.bucket}/{args.src_key}")
        t1 = perf_counter()
        download_minio_object(client, args.bucket, args.src_key, src_local)
        dl_time = perf_counter() - t1
        print(f"  \t downloaded to {src_local} in {dl_time:.2f} seconds")
        sourcename   = os.path.basename(args.src_key).replace(".jsonl", "") 
        print(f"[2/4] Cleaning (limit={args.limit}, min_chars={args.min_chars}, min_thai_ratio={args.min_thai_ratio})")
        t2 = perf_counter()
        stats = process_jsonl_file(
            in_path=src_local,
            out_clean_path=out_clean_local,
            out_logs_path=out_logs_local,
            min_chars=args.min_chars,
            min_thai_ratio=args.min_thai_ratio,
            limit=args.limit,
            object_path=args.src_key, 
            bucket=args.bucket,       
            source_name=sourcename
            )
        cleaning_time = perf_counter() - t2
        print(f"  \t summarize: File in={stats['in']}, cleaned={stats['cleaned']}, failed={stats['failed']}"
              f"(clean step {cleaning_time:.2f} seconds)")
        print(f"[3/4] ๊Uploading cleaned file → s3://{args.bucket}/{args.out_clean_key}")
        t3 = perf_counter()
        upload_minio_object(client, args.bucket, args.out_clean_key, out_clean_local)
        ul_time = perf_counter() - t3
        print(f"  \t uploaded in {ul_time:.2f} seconds")

        print(f"[4/4] Uploading logs    → s3://{args.bucket}/{args.out_logs_key}")
        t4 = perf_counter()
        upload_minio_object(client, args.bucket, args.out_logs_key, out_logs_local)
        log_time = perf_counter() - t4
        print(f"  \t uploaded in {log_time:.2f} seconds")
        total_time = perf_counter() - sum_time

        print(f"✓ เสร็จสิ้น (total {total_time:.2f}s | download {dl_time:.2f}s | clean {cleaning_time:.2f}s | "
          f"upload(cleaned) {ul_time:.2f}s | upload(logs) {log_time:.2f}s)")
if __name__ == "__main__":
    sys.exit(main() or 0)
