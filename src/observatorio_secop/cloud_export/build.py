"""Own-attempt supervision and atomic no-overwrite publication of local packages."""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from observatorio_secop.publication.readers import resolve
from observatorio_secop.warehouse.config import load_warehouse_config

from .budget import Budget
from .contract import load_contract
from .errors import ExportError
from .identity import export_identity, writer_recipe
from .minimization import load_approval, public_policy
from .package import canonical_json, sha256
from .reader import checked

EXPORT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")


@dataclass
class ScratchLifetime:
    path: Path
    confirmed_exit: bool = True


_LIFETIME: ContextVar[ScratchLifetime | None] = ContextVar("export_scratch", default=None)


@contextmanager
def owned_scratch(prefix: str) -> Iterator[Path]:
    scratch = Path(tempfile.mkdtemp(prefix=prefix))
    lifetime = ScratchLifetime(scratch)
    token = _LIFETIME.set(lifetime)
    try:
        yield scratch
    finally:
        _LIFETIME.reset(token)
        # Positive exit confirmation controls lifetime, even if journal I/O fails.
        if lifetime.confirmed_exit:
            shutil.rmtree(scratch)


def _exited(process: subprocess.Popen[Any]) -> bool:
    try:
        return process.poll() is not None
    except Exception:
        return False


def _cancel(process: subprocess.Popen[Any]) -> bool:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if _exited(process):
            return True
        with suppress(Exception):
            os.killpg(process.pid, sig)
        try:
            process.wait(timeout=2)
            return True
        except Exception:
            pass
    return _exited(process)


def phase(mode: str, request: dict[str, Any], scratch: Path, budget: Budget) -> dict[str, Any]:
    request_path = scratch / f"{mode}-request.json"
    result_path = scratch / f"{mode}-result.json"
    baseline_path = scratch / f"{mode}-baseline.json"
    request = {
        **request,
        "result": str(result_path),
        "baseline_file": str(baseline_path),
        "remaining_seconds": budget.remaining_ms(300_000) / 1000,
    }
    request_path.write_bytes(canonical_json(request))
    request_path.chmod(0o600)
    with (scratch / f"{mode}-error.json").open("w+") as errors:
        lifetime = _LIFETIME.get()
        if lifetime is not None and lifetime.path == scratch:
            lifetime.confirmed_exit = False
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "observatorio_secop.cloud_export.worker",
                mode,
                str(request_path),
            ],
            stdout=subprocess.DEVNULL,
            stderr=errors,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                budget.check()
                if baseline_path.exists():
                    baseline = json.loads(baseline_path.read_bytes())["baseline"]
                    status = Path(f"/proc/{process.pid}/status")
                    try:
                        current = next(
                            int(v.split()[1]) * 1024
                            for v in status.read_text().splitlines()
                            if v.startswith("VmRSS:")
                        )
                    except (FileNotFoundError, StopIteration):
                        current = baseline
                    if current - baseline > budget.memory_bytes:
                        raise ExportError("EXPORT_LIMIT", "Worker observed memory budget exceeded")
                # Scratch may include full private validation IPC; admit it separately
                # from the 80 MiB public projection package, with a fixed 512 MiB cap.
                if (
                    sum(p.stat().st_size for p in scratch.rglob("*") if p.is_file())
                    > 512 * 1024 * 1024
                ):
                    raise ExportError("EXPORT_LIMIT", "Private validation scratch exceeds limit")
                time.sleep(0.1)
            budget.check()
            if process.returncode:
                if mode != "verify" and (scratch / "failure.json").exists():
                    shutil.copyfile(
                        scratch / "failure.json", Path(request["stage"]) / "failure.json"
                    )
                errors.seek(0)
                try:
                    code = json.loads(errors.read(1024)).get("code", "EXPORT_INCOMPLETE")
                except ValueError:
                    code = "EXPORT_INCOMPLETE"
                if not re.fullmatch(r"(EXPORT|PUBLICATION)_[A-Z_]+", str(code)):
                    code = "EXPORT_INCOMPLETE"
                raise ExportError(code, "Bounded export phase did not complete")
            return cast(dict[str, Any], json.loads(result_path.read_bytes()))
        finally:
            original_failure = sys.exception()
            confirmed = _exited(process) or _cancel(process)
            if lifetime is not None and lifetime.path == scratch:
                lifetime.confirmed_exit = confirmed
            if not confirmed:
                # Journaling is best effort and never grants cleanup permission.
                marker = scratch / "worker-unreaped.json"
                try:
                    marker.write_bytes(
                        canonical_json(
                            {
                                "pid": process.pid,
                                "phase": mode,
                                "termination_confirmed": False,
                                "request_file": request_path.name,
                            }
                        )
                    )
                    marker.chmod(0o600)
                    if mode != "verify":
                        shutil.copyfile(marker, Path(request["stage"]) / "cancellation.json")
                except Exception:
                    pass
                code = getattr(original_failure, "code", "EXPORT_INCOMPLETE")
                raise ExportError(
                    code, "Worker exit is not confirmed; owned scratch retained"
                ) from None


def build(
    *,
    config_path: Path,
    silver_root: Path,
    output: Path,
    publication_id: str,
    export_id: str | None = None,
    label_null_approval: Path | None = None,
) -> dict[str, Any]:
    if export_id is not None and not EXPORT_ID.fullmatch(export_id):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Invalid export identifier")
    contract = load_contract()
    config = load_warehouse_config(config_path)
    descriptor = resolve(config, publication_id)
    binding = {
        k: descriptor["metadata"][k]
        for k in ("silver_version_id", "silver_manifest_sha256", "scope_id")
    }
    writer = writer_recipe()
    approval = load_approval(label_null_approval, descriptor)
    minimization = public_policy(approval)
    canonical = export_identity(publication_id, binding, contract.digest, writer, minimization)
    if export_id is not None and export_id != canonical:
        raise ExportError(
            "EXPORT_INPUT_CONFLICT",
            "Requested export ID differs from canonical input/writer identity",
        )
    export_id = canonical
    if output.is_symlink():
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Output root cannot be a symlink")
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output.stat().st_mode & 0o077:
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Output root must be private (0700)")
    target = output / export_id
    lock_path = output / f".lock-{export_id}"
    # O_NOFOLLOW also prevents a caller-provided lock symlink escaping the root.
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(lock_fd, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ExportError(
                "EXPORT_PACKAGE_CONFLICT", "Another builder owns this export ID"
            ) from error
        if target.exists() or target.is_symlink():
            existing = supervised_verify(target)
            expected = {
                k: descriptor["metadata"][k]
                for k in ("silver_version_id", "silver_manifest_sha256", "scope_id")
            }
            if existing["publication_id"] != publication_id or existing["binding"] != expected:
                raise ExportError(
                    "EXPORT_PACKAGE_CONFLICT", "Export identifier already has another binding"
                )
            return {
                "status": "unchanged",
                "export_id": export_id,
                "manifest_sha256": sha256(target / "manifest.json"),
            }
        # This directory remains recognizable as incomplete on any failure. Scratch
        # is a separate owned temporary directory, always removed after workers exit.
        stage = Path(tempfile.mkdtemp(prefix=f".staging-{export_id}-", dir=output))
        budget = Budget()
        with owned_scratch("secop-export-") as scratch:
            request = {
                "config": str(config_path.resolve()),
                "silver_root": str(silver_root.resolve()),
                "stage": str(stage.resolve()),
                "scratch": str(scratch),
                "descriptor": descriptor,
                "export_id": export_id,
                "writer": writer,
                "minimization_approval": approval,
                "minimization": minimization,
            }
            validated = phase("validate", request, scratch, budget)
            for owned in scratch.glob("*.arrow"):
                owned.unlink()
            (scratch / "silver-exact.sqlite").unlink(missing_ok=True)
            exported = phase(
                "export", {**request, "silver_summary": validated["result"]}, scratch, budget
            )
            checked(config, descriptor, budget)
            budget.files(stage)
            if writer_recipe() != writer:
                raise ExportError("EXPORT_INPUT_CONFLICT", "Writer recipe changed before seal")
            if target.exists() or target.is_symlink():
                raise ExportError(
                    "EXPORT_PACKAGE_CONFLICT", "Destination appeared during construction"
                )
            budget.check()
            # Cooperating builders hold the per-ID advisory file lock. renameat2
            # RENAME_NOREPLACE refuses even a non-cooperating destination racer.
            _rename_no_replace(stage, target)
        return {
            "status": "built",
            "export_id": export_id,
            "manifest_sha256": sha256(target / "manifest.json"),
            "rows": sum(v["rows"] for v in exported["result"]["relations"].values()),
            "files": len(exported["result"]["files"]) + 1,
            "bytes": sum(p.stat().st_size for p in target.rglob("*") if p.is_file()),
            "seconds": round(time.monotonic() - budget.started, 3),
            "peak_increment_bytes": max(validated["peak_increment"], exported["peak_increment"]),
        }


def _rename_no_replace(source: Path, target: Path) -> None:
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.renameat2(-100, os.fsencode(source), -100, os.fsencode(target), 1)
    if result:
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Atomic destination publication refused")


def supervised_verify(package: Path) -> dict[str, Any]:
    with owned_scratch("secop-export-verify-") as scratch:
        result = phase(
            "verify",
            {"package": str(package.absolute()), "scratch": str(scratch)},
            scratch,
            Budget(),
        )
        return cast(dict[str, Any], result["result"])
