import hashlib

def generate_ingestion_id(file_path: str, file_type: str) -> str:
    raw = f"{file_path}|{file_type}"
    return hashlib.sha256(raw.encode()).hexdigest()