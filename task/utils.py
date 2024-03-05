import hashlib
import tempfile
import shutil
import time

from pathlib import Path
from functools import wraps


def retry(func):
    """
    A decorator for retrying a function.

    Parameters
    ----------
    func : function
        The function to be retried.

    Returns
    ----------
    function
        The decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        error = None
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                time.sleep(1)
                error = e
        raise error
    return wrapper



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


def require_cmd(cmd: str):
    cmd_path = shutil.which(cmd)
    if cmd_path is None:
        raise FileNotFoundError(f"Command {cmd} not found.")
    return cmd_path


def wrapper_command_as_script(command: str) -> Path:
    if Path(command).exists():
        return Path(command)
    script = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
    script.write('#!/usr/bin/env bash\n'.encode())
    script.write(command.encode())
    script.close()
    return Path(script.name)
