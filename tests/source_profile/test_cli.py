from __future__ import annotations

from pathlib import Path

from observatorio_secop.source_profile.cli import main
from observatorio_secop.source_profile.errors import SourceUnavailableError

ROOT = Path(__file__).resolve().parents[2]


def test_live_failure_does_not_replace_existing_artifacts(monkeypatch, tmp_path: Path) -> None:
    contract = tmp_path / "contract.yaml"
    fixture = tmp_path / "fixture.json"
    report = tmp_path / "report.md"
    contract.write_text("existing contract\n", encoding="utf-8")
    fixture.write_text("existing fixture\n", encoding="utf-8")
    report.write_text("existing report\n", encoding="utf-8")

    def fail(*_args: object, **_kwargs: object) -> None:
        raise SourceUnavailableError("source unavailable")

    monkeypatch.setattr("observatorio_secop.source_profile.cli.observe_source", fail)
    result = main(
        [
            "--config",
            str(ROOT / "config" / "secop_source.yaml"),
            "live",
            "--write",
            "--contract",
            str(contract),
            "--fixture",
            str(fixture),
            "--report",
            str(report),
        ]
    )

    assert result == 2
    assert contract.read_text(encoding="utf-8") == "existing contract\n"
    assert fixture.read_text(encoding="utf-8") == "existing fixture\n"
    assert report.read_text(encoding="utf-8") == "existing report\n"
