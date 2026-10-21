"""Security gate behavior uses synthetic findings; no secret literals are tracked."""

import hashlib
import importlib.util
import json
import subprocess
from datetime import date
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "ci_security", Path(__file__).parents[1] / "scripts/ci_security.py"
)
security = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(security)


def policy():
    return {"dependencies": [], "secrets": []}


def finding():
    return {
        "dependencies": [
            {
                "name": "sample",
                "version": "1.0",
                "vulns": [{"id": "PYSEC-2099-1", "aliases": ["CVE-2099-0001"]}],
            }
        ]
    }


def exception():
    return {
        "package": "sample",
        "version": "1.0",
        "advisories": ["PYSEC-2099-1", "CVE-2099-0001"],
        "rationale": "synthetic test",
        "mitigation": "test only",
        "owner": "reviewer",
        "approved_by": "human-test",
        "reviewed": "2026-01-01",
        "expires": "2099-01-01",
    }


def test_unexcepted_dependency_blocks_and_exact_exception_is_visible():
    assert security.dependency_findings(finding(), policy())[0]["excepted"] is False
    approved = policy()
    approved["dependencies"] = [exception()]
    assert security.dependency_findings(finding(), approved)[0]["excepted"] is True


@pytest.mark.parametrize(
    "field,value", [("package", "other"), ("version", "2.0"), ("advisories", ["CVE-2099-0002"])]
)
def test_exception_must_match_exact_package_version_and_all_advisories(field, value):
    approved = policy()
    entry = exception()
    entry[field] = value
    approved["dependencies"] = [entry]
    assert security.dependency_findings(finding(), approved)[0]["excepted"] is False


@pytest.mark.parametrize("expiry", ["2026-01-01", "2026-10-21", "invalid"])
def test_expired_or_invalid_exception_fails_closed(tmp_path, expiry):
    approved = policy()
    entry = exception()
    entry["expires"] = expiry
    approved["dependencies"] = [entry]
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(approved))
    with pytest.raises(security.GateError):
        security.exceptions(path, date(2026, 10, 21))


@pytest.mark.parametrize(
    "report",
    [
        {},
        {"dependencies": []},
        {"dependencies": [{"name": "sample", "skip_reason": "unavailable"}]},
    ],
)
def test_incomplete_audit_is_not_clean(report):
    with pytest.raises(security.GateError):
        security.dependency_findings(report, policy())


def test_tool_error_output_never_reaches_summary(tmp_path, monkeypatch, capsys):
    sensitive = "do-not-publish-this-diagnostic"
    monkeypatch.setattr(
        security,
        "scan_dependencies",
        lambda *args: security.run(
            ["python", "-c", f"import sys; print('{sensitive}'); sys.exit(2)"], tmp_path
        ),
    )
    summary = tmp_path / "summary.json"
    assert security.main(["dependencies", "--summary", str(summary)]) == 2
    assert sensitive not in summary.read_text() + capsys.readouterr().out
    assert json.loads(summary.read_text())["error"] == "tool_failed"


def test_missing_report_never_counts_as_clean(tmp_path):
    with pytest.raises(security.GateError, match="invalid_or_missing_report"):
        security.read_json(tmp_path / "missing.json")


def test_shallow_history_is_denied_before_scanner_download(tmp_path, monkeypatch):
    monkeypatch.setattr(security, "run", lambda *args: subprocess.CompletedProcess([], 0, b"true"))
    monkeypatch.setattr(security, "install_gitleaks", lambda *args: pytest.fail("downloaded"))
    with pytest.raises(security.GateError, match="incomplete_git_history"):
        security.scan_secrets(tmp_path, tmp_path, policy())


def test_secret_identifiers_and_output_are_minimized():
    sensitive = "sensitive-filename"
    report = [
        {
            "RuleID": "generic-api-key",
            "Fingerprint": sensitive,
            "File": sensitive,
            "Secret": "synthetic-value",
            "Match": "synthetic-value",
        }
    ]
    output = security.secret_findings(report, policy())
    assert output[0]["excepted"] is False
    assert sensitive not in json.dumps(output)
    approved = policy()
    approved["secrets"] = [{"fingerprint": sensitive, "rule": "generic-api-key", "path": sensitive}]
    assert security.secret_findings(report, approved)[0]["excepted"] is True


def test_historical_deleted_secret_detected_and_redacted(tmp_path, monkeypatch):
    """Real Gitleaks execution, using only an explicitly supplied verified local binary."""
    import os

    binary = os.environ.get("CI_SECURITY_TEST_GITLEAKS")
    if not binary:
        pytest.skip("set CI_SECURITY_TEST_GITLEAKS to run the real scanner negative")
    monkeypatch.setattr(security, "install_gitleaks", lambda _: Path(binary))
    repo = tmp_path / "repo"
    repo.mkdir()
    for args in (
        ["init"],
        ["config", "user.email", "synthetic@example.invalid"],
        ["config", "user.name", "Synthetic"],
    ):
        security.run(["git", *args], repo)
    secret = "ghp_" + hashlib.sha256(b"US07 synthetic scanner negative").hexdigest()[:36]
    path = repo / "sample.txt"
    path.write_text("access_token = '" + secret + "'\n")
    security.run(["git", "add", "."], repo)
    security.run(["git", "commit", "-m", "synthetic"], repo)
    current = security.scan_secrets(repo, tmp_path, policy())
    assert current["findings"]
    assert secret not in json.dumps(current)
    path.unlink()
    security.run(["git", "add", "-u"], repo)
    security.run(["git", "commit", "-m", "remove synthetic"], repo)
    output = security.scan_secrets(repo, tmp_path, policy())
    assert output["findings"] and any(not item["excepted"] for item in output["findings"])
    assert secret not in json.dumps(output)
    raw = (tmp_path / "secrets.json").read_text()
    assert secret not in raw


def test_download_network_failure_is_closed(monkeypatch, tmp_path):
    def unavailable(*args, **kwargs):
        raise OSError("sensitive-network-details")

    monkeypatch.setattr(security.urllib.request, "urlopen", unavailable)
    with pytest.raises(security.GateError, match="scanner_download_failed"):
        security.install_gitleaks(tmp_path)


def test_download_checksum_failure_is_closed(monkeypatch, tmp_path):
    import io

    monkeypatch.setattr(security.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"tampered"))
    with pytest.raises(security.GateError, match="scanner_checksum_mismatch"):
        security.install_gitleaks(tmp_path)


def test_usable_secret_exception_cannot_be_authorized(tmp_path):
    approved = policy()
    approved["secrets"] = [
        {
            "fingerprint": "synthetic",
            "rule": "rule",
            "path": "file",
            "false_positive": False,
            "rationale": "test",
            "mitigation": "test",
            "owner": "test",
            "approved_by": "test",
            "reviewed": "2026-01-01",
            "expires": "2099-01-01",
        }
    ]
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(approved))
    with pytest.raises(security.GateError, match="usable_secret_exception_forbidden"):
        security.exceptions(path, date(2026, 10, 21))
