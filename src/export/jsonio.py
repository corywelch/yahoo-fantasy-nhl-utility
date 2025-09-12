from pathlib import Path
import json
from typing import Any

def dump_json(obj: Any, path: Path, pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.write("\n")
        else:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
