import hashlib

from pathlib import Path


def sha256sum(file_path: Path, length: int = 64) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256.update(byte_block)
    return sha256.hexdigest()[:length]

def sha256sum_str(string: str, length: int = 64) -> str:
    sha256 = hashlib.sha256()
    sha256.update(string.encode())
    return sha256.hexdigest()[:length]
