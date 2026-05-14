# ingest_precomputed_minio.py #try.py
import os, json, uuid, argparse, tempfile,traceback , sys
import psycopg
import uuid as _uuid
from psycopg.types.json import Json
from psycopg.rows import dict_row
from minio import Minio
from utils import get_minio_client, download_minio_object  # คุณมีอยู่แล้ว
import sys
# ensure logs flush immediately
if hasattr(sys.stdout, "reconfigure"): sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"): sys.stderr.reconfigure(line_buffering=True)
def normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    return dsn
def must_uuid(v):
    if not v:
        raise ValueError("missing id in JSONL line")
    import uuid
    return str(uuid.UUID(str(v))) 

def probe_documents_table(dsn: str):
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
          SELECT column_name, is_nullable, column_default, data_type
          FROM information_schema.columns
          WHERE table_schema='public' AND table_name='documents'
        """)
        cols = {r["column_name"]: r for r in cur.fetchall()}
        return cols
def getname(path: str, dsn: str) -> str:
    p = path.strip("/")
    parent = p.split(".")[0]
    parts = parent.split("/")
    with psycopg.connect(dsn) as conn: 
        conn.autocommit=True
        with conn.cursor() as cur:
            for i in parts[::-1]: 
                cur.execute("SELECT id  FROM datasets WHERE name = %s ", (i,))
                row = cur.fetchone()
                if row:
                    for i,a in enumerate(row):
                        return a
                    break
    raise RuntimeError(f"dataset id not found from path: {path}")
def doc_exists_by_checksum(cur, checksum: str) -> bool:
    if not checksum:
        return False
    cur.execute("SELECT 1 FROM documents WHERE checksum = %s LIMIT 1", (checksum,))
    return cur.fetchone() is not None
def ingest_from_minio(dsn: str,bucket: str,path: str,dataset_id: str | None ,limit: int , content: bool = True,):
    upsert = True
    client  = get_minio_client() 
    inserted = 0
    skip = 0

    with tempfile.TemporaryDirectory(prefix="ingest_pre_") as tmpdir:
        local_path = os.path.join(tmpdir, "src.jsonl")
        print(f" Download: s3://{bucket}/{path} -> {local_path}")
        download_minio_object(client, bucket, path, local_path)
        dsn = normalize_dsn(dsn)
        
        try:
            with psycopg.connect(dsn) as conn:
                conn.autocommit = True
                with conn.cursor(row_factory=dict_row) as cur, open(local_path, "r", encoding="utf-8") as fi:
                    for i, line in enumerate(fi, start=1):
                        if limit and i > limit:
                            break
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            print(f"skip line {i}: JSON decode error")
                            continue
                        status_final = "ingested"

                        # ----- ใช้ค่าที่มีอยู่แล้วจากไฟล์ cleaned/mapped -----
                        id         = obj.get("id") or str(_uuid.uuid4())  
                        title      = obj.get("filename") or obj.get("title") or f"{obj.get('source','unknown')}:{i}"
                        language   = obj.get("language") or "th"
                        source     = obj.get("source") or "unknown_source"
                        content_ptr= obj.get("content_ptr")
                        checksum   = obj.get("checksum")   # ถ้าไม่มี ปล่อย None
                        word_count = obj.get("word_count", 0)
                        char_count = obj.get("char_count", 0)
                        thai_ratio = obj.get("thai_ratio", 0.0)
                        status     = obj.get("status", "validated")
                        metadata   = obj.get("metadata") or {}
                        text       = obj.get("text") or ""
                        if checksum and doc_exists_by_checksum(cur, checksum):
                            skip += 1       # Optional verbose:
                        # print(f"[SKIP] already ingested checksum={checksum}")
                            continue
                    
                        sql = """
                        INSERT INTO documents
                        ("id","dataset_id","title","content","content_ptr","language","source","checksum",
                        "word_count","char_count","thai_ratio","metadata","status")
                        VALUES (%(id)s,%(dataset_id)s,%(title)s,%(content)s,%(content_ptr)s,%(language)s,%(source)s,%(checksum)s,
                                %(word_count)s,%(char_count)s,%(thai_ratio)s,%(metadata)s,%(status)s)
                        """
                        if upsert:
                            sql += """
                            ON CONFLICT (checksum) DO UPDATE SET
                              dataset_id = EXCLUDED.dataset_id,
                              title      = EXCLUDED.title,
                              content_ptr= EXCLUDED.content_ptr,
                              language   = EXCLUDED.language,
                              source     = EXCLUDED.source,
                              word_count = EXCLUDED.word_count,
                              char_count = EXCLUDED.char_count,
                              thai_ratio = EXCLUDED.thai_ratio,
                              metadata   = EXCLUDED.metadata,
                              status     = 'ingested',
                              content    = EXCLUDED.content
                            """
                            print("Executing upsert based on checksum")
                        try:
                            cur.execute(sql, {
                            "id": id,
                            "dataset_id": dataset_id,
                            "title": title,
                            "content_ptr": Json(content_ptr),
                            "language": language,
                            "source": source,
                            "checksum": checksum,
                            "word_count": word_count,
                            "char_count": char_count,
                            "thai_ratio": thai_ratio,
                            "metadata": Json(metadata),
                            "status": status_final,
                            "content": text if content else "None",       # เอาออกด้วยถ้าไม่ต้องการtext
                        })
                        except psycopg.Error as e:
                            print("\n--- DB ERROR ---")
                            print("type:", type(e).__name__)
                            print("msg:", str(e))
                        inserted += 1
        except Exception:
            safe_dsn = dsn
            if "@" in safe_dsn:
                safe_dsn = "****@" + safe_dsn.split("@", 1)[1]
            print(f"[ERROR] Ingest failed. dsn={safe_dsn}")
            traceback.print_exc()
            sys.exit(1)
    print(f"Done. Inserted {inserted} row (s), skipped {skip} row(s) due to existing checksum.")
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn",default="postgresql://postgres:sukiepal@postgres:5432/eiei")
    ap.add_argument("--bucket",default="bucket")
    ap.add_argument("--path", required=True)
    ap.add_argument("--limit", type=int,)
    ap.add_argument("--text", action="store_true", help="เพิ่ม field text ลงในฐานข้อมูล")
    ap.add_argument("--dataset-id",default= None, help="ระบุ dataset id โดยตรง (ถ้ามี)")
    args = ap.parse_args()
    dsn = normalize_dsn(args.dsn)
    #dataset_id = getname(args.path,args.dsn)
    try: 
        ingest_from_minio(
            dsn=args.dsn,
            bucket=args.bucket,
            dataset_id=args.dataset_id,
            path=args.path,
            limit=args.limit,
            content =  args.text,
        )
        pass
    except Exception:
        print("[TRY] Uncaught exception in try.py:")
        traceback.print_exc()
        sys.exit(1)