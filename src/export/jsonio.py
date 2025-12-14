"""JSON input/output utilities."""

from pathlib import Path
import json
from typing import Any

def dump_json(data: Any, path: Path, pretty: bool = False) -> None:
    """Write JSON data to file.

    Args:
        data: Data to serialize as JSON
        path: Destination file path
        pretty: Whether to use pretty formatting with indentation
    """
    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        if pretty:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.write("\n")
        else:
            json.dump(data, file, ensure_ascii=False, separators=(",", ":"))
