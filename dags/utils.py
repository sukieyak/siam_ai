import os, hashlib, re, sys
from datetime import datetime
from urllib.parse import urlparse 
from zoneinfo import ZoneInfo
from minio import Minio
from dotenv import load_dotenv


load_dotenv()
THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
# def get_minio_client():
#     endpoint = os.environ["MINIO_ENDPOINT"]  
#     access_key=os.environ["MINIO_ACCESS_KEY"]
#     secret_key=os.environ["MINIO_SECRET_KEY"]

#     if not access_key or not secret_key:
#         raise RuntimeError("Can't find minio_key in env")
#     secure = endpoint.startswith("https://")
#     host   = endpoint.replace("http://", "").replace("https://", "")
#     #return Minio(host, access_key=access_key, secret_key=secret_key, secure=secure)
#     return Minio(
#         endpoint=endpoint,
#         access_key=access_key,
#         secret_key=secret_key,
#         secure=secure,
#     )
def get_minio_client():
    # อ่านค่าจาก ENV (ถ้าไม่มีใช้ค่า default เป็น minio:9000)
    raw_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        print("ERROR: MINIO_ACCESS_KEY / MINIO_SECRET_KEY ไม่ถูกตั้งค่า", file=sys.stderr)
        sys.exit(2)

    # แปลง endpoint ให้ชัวร์ว่าไม่มี path
    parsed = urlparse(raw_endpoint)

    if parsed.scheme:  # กรณีแบบ http://minio:9000 หรือ http://192.168.1.10:9010/minio
        # ตัด path ทิ้ง เอาแค่ host:port
        endpoint = parsed.netloc          # เช่น "minio:9000" หรือ "192.168.1.10:9010"
        secure = parsed.scheme == "https"
    else:
        # กรณีแบบ "minio:9000" หรือ "minio:9000/minio"
        endpoint = raw_endpoint.split("/")[0]  # ตัดทุกอย่างหลัง / ทิ้ง
        secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"

    # debug ให้เห็นค่าที่ใช้จริง
    print(f"[DEBUG] RAW MINIO_ENDPOINT={raw_endpoint}", file=sys.stderr)
    print(f"[DEBUG] USING endpoint={endpoint}, secure={secure}", file=sys.stderr)

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

def normalize_key(*parts: str) -> str:
    key = "/".join(
        p.strip().strip("/").replace("\\", "/")
        for p in parts if p and p.strip("/")
    )
    # กันเคสยังหลงเหลือ '//' (เผื่อมีช่องว่างว่าง ๆ)
    while '//' in key:
        key = key.replace('//','/')
    return key.lstrip('/')  # ห้ามขึ้นต้นด้วย '/'

def download_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str):
    key = normalize_key(key)          # กัน key ผิดทุกครั้ง
    minio_client.fget_object( bucket_name=bucket,
        object_name=key,
        file_path=local_path,)

# def upload_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str, content_type: str = "application/jsonl"):

#     found = minio_client.bucket_exists(bucket)
#     if not found:
#         minio_client.make_bucket(bucket)
#     minio_client.fput_object(bucket_name=bucket,
#         object_name=key,
#         file_path=local_path,
#         content_type=content_type,)
def upload_minio_object(minio_client: Minio, bucket: str, key: str, local_path: str, content_type: str = "application/jsonl"):
    # เช็คว่ามี bucket นี้หรือยัง → ใช้ keyword args
    found = minio_client.bucket_exists(bucket_name=bucket)
    if not found:
        minio_client.make_bucket(bucket_name=bucket)

    # อัพโหลดไฟล์ขึ้น MinIO → ใช้ keyword args แล้ว (อันนี้คุณแก้ไว้ถูกแล้ว)
    minio_client.fput_object(
        bucket_name=bucket,
        object_name=key,
        file_path=local_path,
        content_type=content_type,
    )
def checksum_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def now_thai() -> str:
    return datetime.now(ZoneInfo("Asia/Bangkok")).replace(microsecond=0).isoformat()

def thai_ratio(text: str) -> float:
    if not text:
        return 0.0
    return len(THAI_RE.findall(text)) / max(1, len(text))
# utils.py
