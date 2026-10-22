"""Fail-closed reconciliation of collected critical nodeids and all three phases."""

from __future__ import annotations

import json
import math
from pathlib import Path


class GateError(ValueError):
    """Evidence is absent, malformed or does not establish required execution."""


def reconcile(report: dict, inventory: dict) -> dict:
    try:
        collected = report["collected"]
        deselected = report["deselected"]
        results = report["results"]
        assert type(report["schema"]) is int and report["schema"] == 1
        assert isinstance(collected, list) and collected
        assert all(isinstance(n, str) for n in collected + deselected)
        assert len(collected) == len(set(collected))
        assert isinstance(results, dict)
        for phases in results.values():
            assert isinstance(phases, dict)
            for phase, outcome in phases.items():
                assert phase in {"setup", "call", "teardown"}
                assert isinstance(outcome, dict)
                assert outcome.get("outcome") in {"passed", "failed", "skipped"}
                duration = outcome.get("duration", 0)
                assert type(duration) in (int, float) and math.isfinite(duration) and duration >= 0
        assert type(report["collection_errors"]) is int and report["collection_errors"] >= 0
        assert type(report["exitstatus"]) is int and 0 <= report["exitstatus"] <= 5
        seeds = inventory["required_nodeids"]
        families = inventory["critical_families"]
        assert seeds and families
        assert all(isinstance(n, str) for n in seeds + families)
    except (KeyError, TypeError, AssertionError) as error:
        raise GateError("malformed execution evidence") from error
    missing = set(seeds) - set(collected)
    failures = bool(missing or report["collection_errors"] or report["exitstatus"])
    # Every collected test needs terminal evidence, including permitted noncritical skips.
    for node in collected:
        phases = results.get(node, {})
        setup = phases.get("setup", {}).get("outcome")
        terminal = "setup" in phases and "teardown" in phases
        terminal &= "call" in phases or setup in {"failed", "skipped"}
        failures |= not terminal or any(p["outcome"] == "failed" for p in phases.values())
    summaries = {}
    for family in families:
        nodes = [n for n in collected if n.startswith(family)]
        counts = dict.fromkeys(
            ("collected", "executed", "passed", "failed", "errored", "skipped"), 0
        )
        counts["collected"] = len(nodes)
        counts["uncollected"] = sum(n.startswith(family) for n in missing)
        counts["deselected"] = sum(n.startswith(family) for n in deselected)
        failures |= not nodes or bool(counts["deselected"])
        for node in nodes:
            phases = results.get(node)
            if not isinstance(phases, dict):
                failures = True
                counts["errored"] += 1
                continue
            if any(not isinstance(value, dict) for value in phases.values()):
                raise GateError("malformed phase evidence")
            valid = True
            for phase in ("setup", "call", "teardown"):
                outcome = phases.get(phase, {})
                if not isinstance(outcome, dict) or outcome.get("outcome") not in {
                    "passed",
                    "failed",
                    "skipped",
                }:
                    valid = False
                    continue
                if outcome["outcome"] != "passed" or outcome.get("xfail"):
                    valid = False
            counts["executed"] += "call" in phases
            counts["skipped"] += any(p.get("outcome") == "skipped" for p in phases.values())
            counts["failed"] += phases.get("call", {}).get("outcome") == "failed"
            counts["errored"] += any(
                phases.get(p, {}).get("outcome") == "failed" for p in ("setup", "teardown")
            )
            counts["passed"] += valid
            failures |= not valid
        summaries[family] = counts
    if set(results) - set(collected):
        failures = True
    noncritical = [node for node in collected if not any(node.startswith(f) for f in families)]
    omissions = []
    for node in noncritical:
        for phase, outcome in results.get(node, {}).items():
            if outcome["outcome"] == "skipped" or outcome.get("xfail"):
                omissions.append(
                    {
                        "nodeid": node,
                        "phase": phase,
                        "reason": "expected-failure"
                        if outcome.get("xfail")
                        else outcome.get("reason", "test-or-fixture-skip"),
                    }
                )
    return {
        "passed": not failures,
        "missing_nodeids": sorted(missing),
        "families": summaries,
        "noncritical_collected": len(noncritical),
        "noncritical_omissions": omissions,
    }


def load_report(path: Path) -> dict:
    try:
        result = json.loads(path.read_text())
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (OSError, ValueError) as error:
        raise GateError("missing or malformed execution evidence") from error
