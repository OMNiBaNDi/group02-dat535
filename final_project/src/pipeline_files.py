"""Shared helpers for JSON files, checksums and storage paths."""

import hashlib
import json
from pathlib import Path


def file_sha256(path):
    """Hash file bytes without loading a large source into memory."""
    checksum = hashlib.sha256()
    with path.open("rb") as handle:
        # Read 1 MiB at a time and stop when there are no more bytes.
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def json_sha256(value):
    """Hash JSON with sorted keys and no optional whitespace."""
    # Dictionary key order should not change publication or mapping IDs.
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def child_path(base, relative, *, must_exist=False):
    """Resolve a relative path and keep it inside its storage root."""
    base, relative = Path(base).resolve(), Path(relative)
    if relative.is_absolute():
        raise ValueError(f"Expected a relative path: {relative}")
    # Resolve ".." and existing symbolic links before checking the final location.
    result = (base / relative).resolve(strict=must_exist)
    if result == base or not result.is_relative_to(base):
        raise ValueError(f"Path is outside its base directory: {result}")
    return result


def verify_file_inventory(bundle, datasets):
    """Check the listed files and return their paths grouped by dataset.

    Files not listed in the manifest are ignored.
    """
    verified = {}
    for name, info in datasets.items():
        paths = [child_path(bundle, item["path"], must_exist=True) for item in info["files"]]
        if not paths or len(paths) != len(set(paths)):
            raise ValueError(f"{name}: empty or duplicate file inventory")
        for path, item in zip(paths, info["files"]):
            if path.stat().st_size != item["bytes"] or file_sha256(path) != item["sha256"]:
                raise ValueError(f"{name}: file size or checksum differs: {path}")
        verified[name] = [str(path) for path in paths]
    return verified


def write_json(path, value):
    """Write JSON to a temporary file, then replace the destination."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)
