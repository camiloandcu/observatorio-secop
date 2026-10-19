"""Verify bundle evidence and select the exact immutable Silver data file set."""

from __future__ import annotations

import hashlib
import stat
from pathlib import Path
from typing import Any

from observatorio_secop.warehouse.errors import SilverBundleError


def verify_inventory(directory: Path, value: Any) -> tuple[Path, ...]:
    """Return only verified data paths; never follow links or skip hidden entries."""
    try:
        return _verified_files(directory.resolve(), value)
    except (OSError, ValueError, RuntimeError) as error:
        raise SilverBundleError(f"Could not verify Silver inventory: {error}") from error


def _verified_files(directory: Path, value: Any) -> tuple[Path, ...]:
    if not isinstance(value, list) or not value:
        raise SilverBundleError("Silver manifest file inventory is missing")
    declared: set[Path] = set()
    resolved: set[Path] = set()
    for item in value:
        if not isinstance(item, dict):
            raise SilverBundleError("files item must be an object")
        path = _inventory_path(directory, item.get("path"))
        target = path.resolve()
        relative = path.relative_to(directory).as_posix()
        if path in declared or target in resolved:
            raise SilverBundleError(f"Silver inventory path is duplicated: {relative}")
        declared.add(path)
        resolved.add(target)
        size = item.get("size_bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise SilverBundleError("size_bytes must be a non-negative integer")
        if path.stat().st_size != size:
            raise SilverBundleError(f"Silver inventory size differs: {relative}")
        if sha256_path(path) != item.get("sha256"):
            raise SilverBundleError(f"Silver inventory hash differs: {relative}")

    data = directory / "data"
    observed: set[Path] = set()
    for path in _data_files(directory, data):
        relative = path.relative_to(directory).as_posix()
        if path.name.endswith(".parquet"):
            observed.add(path)
        elif path.name == "_SUCCESS":
            if path not in declared:
                raise SilverBundleError(f"Silver undeclared auxiliary file: {relative}")
        elif _is_checksum(path, declared):
            continue
        else:
            raise SilverBundleError(f"Silver unsupported data artifact: {relative}")
    expected = {p for p in declared if p.is_relative_to(data) and p.name.endswith(".parquet")}
    extra = observed - expected
    missing = expected - observed
    if extra:
        names = sorted(p.relative_to(directory).as_posix() for p in extra)
        raise SilverBundleError(f"Silver undeclared data files: {names}")
    if missing:
        names = sorted(p.relative_to(directory).as_posix() for p in missing)
        raise SilverBundleError(f"Silver inventory data files are missing: {names}")
    if not expected:
        raise SilverBundleError("Published Silver data contains no Parquet files")
    return tuple(sorted(expected))


def _inventory_path(directory: Path, value: Any) -> Path:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\\" in value
        or "\0" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or Path(value).is_absolute()
    ):
        raise SilverBundleError(f"Silver inventory path must be canonical and relative: {value!r}")
    path = directory
    for part in value.split("/"):
        path = path / part
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError as error:
            raise SilverBundleError(f"Silver inventory file is missing: {value}") from error
        if stat.S_ISLNK(mode):
            raise SilverBundleError(f"Silver inventory symlink is forbidden: {value}")
    if not path.resolve().is_relative_to(directory) or not stat.S_ISREG(mode):
        raise SilverBundleError(f"Silver inventory file is not a contained regular file: {value}")
    return path


def _data_files(directory: Path, data: Path) -> list[Path]:
    pending = [data]
    files = []
    while pending:
        path = pending.pop()
        relative = path.relative_to(directory).as_posix()
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError as error:
            raise SilverBundleError(f"Silver data entry is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise SilverBundleError(f"Silver data symlink is forbidden: {relative}")
        if stat.S_ISDIR(mode):
            pending.extend(sorted(path.iterdir(), reverse=True))
        elif stat.S_ISREG(mode) and path != data:
            files.append(path)
        else:
            raise SilverBundleError(
                f"Silver data entry is not a regular file/directory: {relative}"
            )
    return files


def _is_checksum(path: Path, declared: set[Path]) -> bool:
    if not path.name.startswith(".") or not path.name.endswith(".crc"):
        return False
    sibling_name = path.name[1:-4]
    if not sibling_name:
        return False
    sibling = path.with_name(sibling_name)
    return sibling in declared and (sibling.name.endswith(".parquet") or sibling.name == "_SUCCESS")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise SilverBundleError(f"Could not hash Silver file {path.name}: {error}") from error
    return digest.hexdigest()
