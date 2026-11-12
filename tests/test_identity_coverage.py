"""Exact coverage semantics and structural protection against quadratic scans."""

from __future__ import annotations

import copy
import json
import random
from decimal import Decimal

import pytest

from observatorio_secop.identity import identity_coverage


def contract(amount="10.01", *, code="001", name="Nombre", known=True):
    value = {"contract_value_cop": Decimal(amount)}
    for role in ["entity", "supplier"]:
        value.update(
            {
                f"{role}_code": code,
                f"{role}_name": name,
                f"{role}_identity_status": "KNOWN" if known else "UNKNOWN",
                f"{role}_identity_reason": None if known else "MISSING_CODE",
            }
        )
    return value


def reference(rows):
    """Small brute-force oracle retaining the pre-optimization grouping rule."""
    total = sum((Decimal(str(r["contract_value_cop"])) for r in rows), Decimal("0.00"))
    output = {}
    for role in ["entity", "supplier"]:
        known = [r for r in rows if r[f"{role}_identity_status"] == "KNOWN"]
        known_value = sum((Decimal(str(r["contract_value_cop"])) for r in known), Decimal("0.00"))
        reasons = sorted(
            {
                r[f"{role}_identity_reason"]
                for r in rows
                if r[f"{role}_identity_status"] == "UNKNOWN"
            }
        )
        output[role] = {
            "label_variant_groups": sum(
                len(
                    {
                        r.get(f"{role}_name")
                        for r in known
                        if r.get(f"{role}_code") == code and r.get(f"{role}_name")
                    }
                )
                > 1
                for code in {r.get(f"{role}_code") for r in known}
            ),
            "denominator": "accepted_deduplicated_contracts",
            "total_count": len(rows),
            "total_value_cop": str(total),
            "known_count": len(known),
            "unknown_count": len(rows) - len(known),
            "known_value_cop": str(known_value),
            "unknown_value_cop": str(total - known_value),
            "coverage_by_count": str(Decimal(len(known)) / len(rows)) if rows else None,
            "coverage_by_value": str(known_value / total) if total else None,
            "count_coverage_reason": None if rows else "EMPTY_POPULATION",
            "value_coverage_reason": None if total else "ZERO_TOTAL_VALUE",
            "unknown_reasons": {
                reason: {
                    "count": sum(
                        r[f"{role}_identity_reason"] == reason
                        for r in rows
                        if r[f"{role}_identity_status"] == "UNKNOWN"
                    ),
                    "value_cop": str(
                        sum(
                            (
                                Decimal(str(r["contract_value_cop"]))
                                for r in rows
                                if r[f"{role}_identity_status"] == "UNKNOWN"
                                and r[f"{role}_identity_reason"] == reason
                            ),
                            Decimal("0.00"),
                        )
                    ),
                }
                for reason in reasons
            },
        }
    return output


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [contract("0.00")],
        [contract("90071992547409.93"), contract("0.01", known=False)],
        [
            contract(code="001", name="A"),
            contract(code="001", name="B"),
            contract(code="001", name="B"),
            contract(code="001", name=""),
            contract(code="001", name=None),
            contract(code="002", name="A"),
            contract(code=None, name="C"),
            contract(code=None, name="D"),
            contract(code="001", name="Unknown name", known=False),
        ],
    ],
)
def test_complete_output_matches_previous_semantics(rows):
    original = copy.deepcopy(rows)
    actual = identity_coverage(rows)
    assert actual == reference(rows)
    assert json.dumps(actual, sort_keys=True) == json.dumps(reference(rows), sort_keys=True)
    assert rows == original


def test_role_specific_names_and_reason_amounts_remain_distinct():
    rows = [contract("10.01"), contract("20.02"), contract("0.00", known=False)]
    rows[1].update(entity_name="Otra entidad", supplier_name="Nombre")
    rows[2].update(
        entity_identity_reason="LEGACY_PROJECTION", supplier_identity_reason="INVALID_CODE"
    )
    actual = identity_coverage(rows)
    assert actual == reference(rows)
    assert actual["entity"]["label_variant_groups"] == 1
    assert actual["supplier"]["label_variant_groups"] == 0
    assert actual["supplier"]["unknown_reasons"]["INVALID_CODE"] == {
        "count": 1,
        "value_cop": "0.00",
    }


def test_seeded_mixed_populations_and_input_permutations_match_reference():
    rng = random.Random(260630)
    for size in [1, 17, 128]:
        rows = []
        for _ in range(size):
            r = contract(
                str(Decimal(rng.randrange(100000)) / 100),
                code=rng.choice([None, "001", "002", "003"]),
                name=rng.choice([None, "", "A", "B", "C"]),
                known=rng.choice([True, False]),
            )
            r["supplier_code"] = rng.choice([None, "007", "008"])
            r["supplier_name"] = rng.choice([None, "", "Proveedor A", "Proveedor B"])
            rows.append(r)
        expected = reference(rows)
        for _ in range(3):
            rng.shuffle(rows)
            assert identity_coverage(rows) == expected


def test_distinct_code_growth_requires_only_linear_mapping_reads():
    class CountingRow(dict):
        reads = 0

        def get(self, key, default=None):
            type(self).reads += 1
            return super().get(key, default)

    observed = []
    for size in [200, 400]:
        rows = [CountingRow(contract(code=str(i))) for i in range(size)]
        CountingRow.reads = 0
        actual = identity_coverage(rows)
        observed.append(CountingRow.reads)
        assert actual["supplier"]["label_variant_groups"] == 0
        assert CountingRow.reads <= 4 * size
    assert observed[1] == 2 * observed[0]
