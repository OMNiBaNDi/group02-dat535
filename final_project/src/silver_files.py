"""Shared helpers for file checksums and paths that stay inside the data folder."""

import hashlib
import json
from pathlib import Path


def file_sha256(path):
    """Return a file checksum without reading the entire file into memory."""
    # Read large files in chunks so checksum calculation uses little memory.
    checksum = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            checksum.update(chunk)
    return checksum.hexdigest()


def silver_json_sha256(value):
    """Hash JSON with sorted object keys and consistent spacing."""
    # Dictionary insertion order should not change the metadata checksum.
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def silver_child_path(base, relative, *, must_exist=False):
    """Resolve a relative path without leaving its base directory."""
    base = Path(base).resolve()
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Expected a contained relative path: {relative}")
    # Resolve symbolic links too, so a relative path cannot lead outside the base folder.
    result = (base / relative).resolve(strict=must_exist)
    if result == base or not result.is_relative_to(base):
        raise ValueError(f"Path is outside its base directory: {result}")
    return result


def verify_silver_file_inventory(bundle, datasets):
    """Check the recorded Parquet files and return their absolute paths."""
    bundle = Path(bundle).resolve(strict=True)
    verified = {}
    for name, info in datasets.items():
        if info["path"] != name:
            raise ValueError(f"{name}: unexpected dataset directory.")
        directory = silver_child_path(bundle, info["path"], must_exist=True)
        paths = [
            silver_child_path(bundle, item["path"], must_exist=True)
            for item in info["files"]
        ]
        # Require a complete, nonduplicated list of Parquet files in the expected dataset directory.
        if (
            not paths
            or len(paths) != len(set(paths))
            or any(
                not path.is_file() or not path.is_relative_to(directory)
                or path.suffix != ".parquet"
                for path in paths
            )
            or set(paths) != {
                path.resolve() for path in directory.rglob("*.parquet")
            }
            or info["bytes"] != sum(item["bytes"] for item in info["files"])
        ):
            raise ValueError(f"{name}: the Parquet file inventory differs.")

        # Check both file sizes and hashes; changed contents may still have the same length.
        for path, item in zip(paths, info["files"]):
            if (
                path.stat().st_size != item["bytes"]
                or file_sha256(path) != item["sha256"]
            ):
                raise ValueError(
                    f"{name}: file size or checksum differs: {path.name}"
                )
        verified[name] = [str(path) for path in paths]
    return verified
