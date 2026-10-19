from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from observatorio_secop.identity import code_identity, identity_coverage, load_identity_rules
from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.errors import SourceContractError

ROOT = Path(__file__).resolve().parents[2]


def test_explicit_old_and_current_profiles_preserve_exact_contract_evidence() -> None:
    registry = CompatibilityRegistry()
    for profile in registry.profiles:
        resolved = registry.resolve("jbjy-vk9h", profile.contract_sha256, list(profile.projection))
        assert resolved == profile
        with pytest.raises(SourceContractError, match="combination"):
            registry.resolve(
                "jbjy-vk9h",
                profile.contract_sha256,
                list(profile.projection) + ["documento_proveedor"],
            )
        with pytest.raises(SourceContractError):
            registry.resolve("jbjy-vk9h", "0" * 64, list(profile.projection))
        with pytest.raises(SourceContractError):
            registry.resolve("other", profile.contract_sha256, list(profile.projection))
        with pytest.raises(SourceContractError):
            registry.resolve(
                "jbjy-vk9h", profile.contract_sha256, list(profile.projection), "future"
            )
    assert hashlib.sha256((ROOT / "contracts/secop_source.yaml").read_bytes()).hexdigest() == (
        registry.profiles[1].contract_sha256
    )
    assert registry.profiles[0].has_identity is False
    assert registry.profiles[1].has_identity is True


@pytest.mark.parametrize("tamper", ["revision", "rules", "evidence"])
def test_tampered_compatibility_or_rule_evidence_is_rejected(tmp_path, tamper) -> None:
    root = tmp_path / "contracts"
    shutil.copytree(ROOT / "contracts", root)
    if tamper == "evidence":
        (root / "evidence/identity_profile_2026-10-19.json").write_text("{}")
        with pytest.raises(ValueError, match="evidence hash"):
            load_identity_rules(root / "identity_rules.yaml")
    else:
        path = root / (
            "revisions/secop_source_v1.yaml" if tamper == "revision" else "identity_rules.yaml"
        )
        path.write_text(path.read_text() + "\n# altered\n")
        with pytest.raises(SourceContractError, match="hash"):
            CompatibilityRegistry(root / "secop_compatibility.yaml")


def test_ingestion_rejects_unregistered_contract_before_acquisition(tmp_path) -> None:
    registry = CompatibilityRegistry()
    for profile in registry.profiles:
        path = ROOT / f"contracts/revisions/{profile.revision.replace('-', '_')}.yaml"
        supported = load_bronze_contract(path, profile.projection)
        assert supported.contract_hash == profile.contract_sha256
    changed = tmp_path / "changed-contract.yaml"
    changed.write_bytes((ROOT / "contracts/secop_source.yaml").read_bytes() + b"\n# unregistered\n")
    with pytest.raises(SourceContractError, match="Unsupported source contract"):
        load_bronze_contract(changed, registry.profiles[1].projection)


@pytest.mark.parametrize("role", ["entity", "supplier"])
@pytest.mark.parametrize(
    ("raw", "code", "reason"),
    [
        (" 00123 ", "00123", None),
        ("1234567890123456789012345", "1234567890123456789012345", None),
        (None, None, "MISSING_CODE"),
        (" ", None, "MISSING_CODE"),
        ("0", None, "PLACEHOLDER_CODE"),
        ("N/A", None, "PLACEHOLDER_CODE"),
        ("123.0", None, "INVALID_CODE"),
        ("1e3", None, "INVALID_CODE"),
        ("ABC", None, "INVALID_CODE"),
        ("abc", None, "INVALID_CODE"),
    ],
)
def test_conservative_codes_never_coerce_identifiers(role, raw, code, reason) -> None:
    assert code_identity(raw, role, load_identity_rules()) == (
        code,
        "KNOWN" if code is not None else "UNKNOWN",
        reason,
    )
    assert code_identity(raw, role, load_identity_rules(), legacy=True) == (
        None,
        "UNKNOWN",
        "LEGACY_PROJECTION",
    )


def test_identity_coverage_uses_exact_values_and_explicit_zero_denominators() -> None:
    rows = [
        {
            "contract_value_cop": "0.10",
            "entity_identity_status": "KNOWN",
            "supplier_identity_status": "UNKNOWN",
            "supplier_identity_reason": "MISSING_CODE",
        },
        {
            "contract_value_cop": "0.20",
            "entity_identity_status": "UNKNOWN",
            "entity_identity_reason": "LEGACY_PROJECTION",
            "supplier_identity_status": "KNOWN",
        },
    ]
    coverage = identity_coverage(rows)
    assert coverage["entity"]["total_value_cop"] == "0.30"
    assert coverage["entity"]["known_value_cop"] == "0.10"
    assert coverage["supplier"]["unknown_reasons"] == {
        "MISSING_CODE": {"count": 1, "value_cop": "0.10"}
    }
    assert identity_coverage([])["entity"]["coverage_by_count"] is None
    rows[0]["contract_value_cop"] = rows[1]["contract_value_cop"] = "0.00"
    zero = identity_coverage(rows)["entity"]
    assert zero["coverage_by_count"] == "0.5"
    assert zero["coverage_by_value"] is None
    assert zero["value_coverage_reason"] == "ZERO_TOTAL_VALUE"
