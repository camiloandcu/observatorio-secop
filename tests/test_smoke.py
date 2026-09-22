from __future__ import annotations

import importlib
from pathlib import Path

import observatorio_secop


def test_package_and_repository_boundaries_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    modules = ("ingestion", "processing", "scoring", "api")

    assert observatorio_secop.__version__
    for module in modules:
        importlib.import_module(f"observatorio_secop.{module}")

    for relative_path in ("dags", "dbt", "tests", "infra", "scripts", "docs", "powerbi"):
        assert (root / relative_path).is_dir(), relative_path
