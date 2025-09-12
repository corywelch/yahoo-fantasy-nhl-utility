import os
from pathlib import Path

def get_export_dir() -> Path:
    return Path(os.getenv("EXPORT_DIR", "./exports")).expanduser().resolve()
