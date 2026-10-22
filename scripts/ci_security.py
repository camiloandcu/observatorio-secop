#!/usr/bin/env python3
"""Fail-closed security gates; raw scanner output never leaves temporary storage."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import platform
import re
import subprocess
import tarfile
import tempfile
import time
import tomllib
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GITLEAKS_VERSION = "8.30.1"
GITLEAKS_SHA256 = "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
AUDIT_VERSION = "2.10.1"
EXTRAS = {"dev", "data", "warehouse", "orchestration", "ml", "api"}
SAFE_ID = re.compile(r"[A-Za-z0-9_.:+/-]{1,200}\Z")


class GateError(Exception):
    """A fixed diagnostic code, never external tool output."""


def run(command: list[str], cwd: Path, allowed: tuple[int, ...] = (0,)):
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, timeout=600, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GateError("tool_unavailable_or_timeout") from exc
    if result.returncode not in allowed:
        raise GateError("tool_failed")
    return result


def read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise GateError("invalid_or_missing_report") from exc


def safe(value):
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise GateError("invalid_report_identifier")
    return value


def exceptions(path: Path, today: date):
    policy = read_json(path)
    if not isinstance(policy, dict) or set(policy) != {"dependencies", "secrets"}:
        raise GateError("invalid_exception_policy")
    for kind, records in policy.items():
        if not isinstance(records, list):
            raise GateError("invalid_exception_policy")
        for record in records:
            required = {"rationale", "mitigation", "owner", "approved_by", "reviewed", "expires"}
            required |= (
                {"advisories", "package", "version"}
                if kind == "dependencies"
                else {"fingerprint", "rule", "path", "false_positive"}
            )
            if not isinstance(record, dict) or set(record) != required:
                raise GateError("invalid_exception_policy")
            if any(
                not isinstance(record[key], str) or not record[key].strip()
                for key in required - {"advisories", "false_positive"}
            ):
                raise GateError("invalid_exception_metadata")
            try:
                reviewed, expires = (
                    date.fromisoformat(record["reviewed"]),
                    date.fromisoformat(record["expires"]),
                )
            except ValueError as exc:
                raise GateError("invalid_exception_date") from exc
            if reviewed > today or expires <= today or expires <= reviewed:
                raise GateError("expired_or_invalid_exception")
            if kind == "secrets" and record["false_positive"] is not True:
                raise GateError("usable_secret_exception_forbidden")
            if kind == "dependencies" and (
                not isinstance(record["advisories"], list)
                or not record["advisories"]
                or any(not isinstance(item, str) for item in record["advisories"])
            ):
                raise GateError("invalid_exception_advisories")
    return policy


def dependency_findings(report, policy):
    if not isinstance(report, dict) or not isinstance(report.get("dependencies"), list):
        raise GateError("invalid_dependency_report")
    dependencies = report["dependencies"]
    if not dependencies:
        raise GateError("empty_dependency_report")
    findings = []
    for dependency in dependencies:
        if not isinstance(dependency, dict) or "skip_reason" in dependency:
            raise GateError("incomplete_dependency_audit")
        package, version = safe(dependency.get("name")), safe(dependency.get("version"))
        vulns = dependency.get("vulns")
        if not isinstance(vulns, list):
            raise GateError("invalid_dependency_report")
        for vuln in vulns:
            advisory = safe(vuln.get("id"))
            aliases = vuln.get("aliases", [])
            if not isinstance(aliases, list):
                raise GateError("invalid_dependency_report")
            ids = {advisory, *(safe(alias) for alias in aliases)}
            approved = any(
                exception["package"] == package
                and exception["version"] == version
                and set(exception["advisories"]) == ids
                for exception in policy["dependencies"]
            )
            findings.append(
                {
                    "package": package,
                    "version": version,
                    "advisories": sorted(ids),
                    "excepted": approved,
                }
            )
    return findings


def secret_findings(report, policy):
    if not isinstance(report, list):
        raise GateError("invalid_secret_report")
    findings = []
    for item in report:
        if not isinstance(item, dict):
            raise GateError("invalid_secret_report")
        rule = safe(item.get("RuleID"))
        fingerprint, path = item.get("Fingerprint"), item.get("File")
        if not isinstance(fingerprint, str) or not isinstance(path, str):
            raise GateError("invalid_secret_report")
        approved = any(
            entry["fingerprint"] == fingerprint and entry["rule"] == rule and entry["path"] == path
            for entry in policy["secrets"]
        )
        # Paths and fingerprints may themselves contain sensitive filenames. Publish only a hash.
        findings.append(
            {
                "rule": rule,
                "identity_sha256": hashlib.sha256(fingerprint.encode()).hexdigest(),
                "excepted": approved,
            }
        )
    return findings


def install_gitleaks(directory: Path) -> Path:
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise GateError("unsupported_scanner_platform")
    url = (
        f"https://github.com/gitleaks/gitleaks/releases/download/v{GITLEAKS_VERSION}/"
        f"gitleaks_{GITLEAKS_VERSION}_linux_x64.tar.gz"
    )
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            archive = response.read(30_000_000)
    except OSError as exc:
        raise GateError("scanner_download_failed") from exc
    if hashlib.sha256(archive).hexdigest() != GITLEAKS_SHA256:
        raise GateError("scanner_checksum_mismatch")
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            member = tar.getmember("gitleaks")
            if not member.isfile():
                raise GateError("invalid_scanner_archive")
            source = tar.extractfile(member)
            if source is None:
                raise GateError("invalid_scanner_archive")
            binary = directory / "gitleaks"
            binary.write_bytes(source.read())
            binary.chmod(0o700)
    except (tarfile.TarError, KeyError) as exc:
        raise GateError("invalid_scanner_archive") from exc
    return binary


def scan_secrets(repo: Path, temp: Path, policy):
    shallow = run(["git", "rev-parse", "--is-shallow-repository"], repo).stdout.strip()
    if shallow != b"false":
        raise GateError("incomplete_git_history")
    run(["git", "rev-parse", "--verify", "HEAD"], repo)
    binary = install_gitleaks(temp)
    if run([str(binary), "version"], repo).stdout.decode().strip() != GITLEAKS_VERSION:
        raise GateError("scanner_version_mismatch")
    report = temp / "secrets.json"
    # Explicit config and ignore path prevent local/repository suppression files being auto-loaded.
    config = temp / "gitleaks.toml"
    config.write_text("[extend]\nuseDefault = true\n")
    ignore = temp / "empty-ignore"
    ignore.write_text("")
    result = run(
        [
            str(binary),
            "git",
            str(repo),
            "--log-opts=--all",
            "--redact=100",
            "--ignore-gitleaks-allow",
            "--gitleaks-ignore-path",
            str(ignore),
            "--config",
            str(config),
            "--report-format=json",
            "--report-path",
            str(report),
            "--exit-code=1",
            "--no-banner",
        ],
        repo,
        (0, 1),
    )
    findings = secret_findings(read_json(report), policy)
    if bool(findings) != (result.returncode == 1):
        raise GateError("inconsistent_secret_result")
    return {
        "scanner": f"gitleaks {GITLEAKS_VERSION}",
        "archive_sha256": GITLEAKS_SHA256,
        "scope": "all fetched refs, complete Git history; no working-tree private files",
        "findings": findings,
    }


def scan_dependencies(repo: Path, temp: Path, policy):
    project = tomllib.loads((repo / "pyproject.toml").read_text())
    extras = set(project["project"]["optional-dependencies"])
    if extras != EXTRAS:
        raise GateError("declared_extras_changed_review_scope")
    requirements = temp / "requirements.txt"
    run(
        [
            "uv",
            "export",
            "--locked",
            "--all-extras",
            "--no-dev",
            "--no-emit-project",
            "--no-hashes",
            "--output-file",
            str(requirements),
        ],
        repo,
    )
    text = requirements.read_text()
    if not text.strip():
        raise GateError("empty_locked_export")
    report = temp / "dependencies.json"
    tool_lock = ROOT / "config/ci_security_tools.txt"
    environment = temp / "audit-env"
    run(["uv", "venv", "--python", "3.12", str(environment)], repo)
    python = environment / "bin/python"
    run(["uv", "pip", "sync", "--python", str(python), "--require-hashes", str(tool_lock)], repo)
    result = run(
        [
            str(environment / "bin/pip-audit"),
            "--no-deps",
            "--disable-pip",
            "--requirement",
            str(requirements),
            "--format=json",
            "--output",
            str(report),
            "--progress-spinner=off",
            "--strict",
            "--cache-dir",
            str(temp / "advisory-cache"),
            "--timeout=30",
        ],
        repo,
        (0, 1),
    )
    data = read_json(report)
    findings = dependency_findings(data, policy)
    if bool(findings) != (result.returncode == 1):
        raise GateError("inconsistent_dependency_result")
    # pip-audit applies environment markers; compare its coverage with a marker-evaluated inventory.
    inventory_code = (
        "import json,sys; from packaging.requirements import Requirement; "
        "r=[Requirement(l.strip()) for l in open(sys.argv[1]) "
        "if l.strip() and not l.lstrip().startswith('#')]; "
        "print(json.dumps({'included': sorted({x.name.lower().replace('_','-') "
        "+ str(x.specifier) for x in r "
        "if x.marker is None or x.marker.evaluate()}), "
        "'excluded': sorted({x.name for x in r if x.marker and not x.marker.evaluate()})}))"
    )
    inventory = json.loads(
        run(
            [
                str(python),
                "-c",
                inventory_code,
                str(requirements),
            ],
            repo,
        ).stdout
    )
    audited = {
        dep["name"].lower().replace("_", "-") + "==" + dep["version"]
        for dep in data["dependencies"]
    }
    if audited != set(inventory["included"]):
        raise GateError("incomplete_dependency_coverage")
    return {
        "scanner": f"pip-audit {AUDIT_VERSION}",
        "extras": sorted(extras),
        "python": "3.12",
        "platform": platform.system() + "/" + platform.machine(),
        "advisory_service": "PyPI JSON API / Python Packaging Advisory Database",
        "export_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "tool_lock_sha256": hashlib.sha256(tool_lock.read_bytes()).hexdigest(),
        "audited_packages": len(audited),
        "marker_exclusions": [safe(name) for name in inventory["excluded"]],
        "findings": findings,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("secrets", "dependencies"))
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument(
        "--exceptions", type=Path, default=ROOT / "config/ci_security_exceptions.json"
    )
    args = parser.parse_args(argv)
    started = time.monotonic()
    summary = {"control": args.kind, "checked_at": datetime.now(UTC).isoformat()}
    try:
        policy = exceptions(args.exceptions, date.today())
        with tempfile.TemporaryDirectory(prefix="secop-security-") as directory:
            scan = scan_secrets if args.kind == "secrets" else scan_dependencies
            summary.update(scan(args.repo.resolve(), Path(directory), policy))
        blocked = sum(not item["excepted"] for item in summary["findings"])
        summary.update(status="blocked" if blocked else "passed", unexcepted=blocked)
        code = int(bool(blocked))
    except (GateError, OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        summary.update(
            status="error",
            error=str(exc) if isinstance(exc, GateError) else "invalid_input_or_report",
        )
        code = 2
    summary["elapsed_seconds"] = time.monotonic() - started
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"security {args.kind}: {summary['status']}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
