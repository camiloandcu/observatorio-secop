"""The real Airflow gate fails closed on missing, skipped or misclassified evidence."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pipeline_gate", ROOT / "scripts/check_pipeline_e2e.py"
)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def test_required_case_inventory_cannot_be_omitted_or_skipped():
    cases = {case: {"status": "passed"} for case in gate.REQUIRED_CASES}
    gate.require_cases(cases)
    for absent in gate.REQUIRED_CASES:
        incomplete = {case: value for case, value in cases.items() if case != absent}
        with pytest.raises(gate.GateError, match="MISSING_OR_FAILED"):
            gate.require_cases(incomplete)
        for state in ("skipped", "xfail", "failed", "not_run"):
            incomplete[absent] = {"status": state}
            with pytest.raises(gate.GateError, match="MISSING_OR_FAILED"):
                gate.require_cases(incomplete)


def test_operator_interrupt_has_a_safe_report_code_and_failed_state():
    report = {"passed": True, "stage": "cold_scheduler_replay"}
    gate.mark_operator_interrupt(report)
    assert report == {
        "passed": False,
        "stage": "interrupted",
        "failure_code": "PIPELINE_E2E_INTERRUPTED_BY_OPERATOR",
    }


def test_all_real_tasks_are_required():
    tasks = [{"task_id": task, "state": "success"} for task in gate.STAGES]
    gate.require_tasks(tasks)
    for state in (None, "skipped", "up_for_retry", "failed"):
        incomplete = [dict(task) for task in tasks]
        incomplete[0]["state"] = state
        with pytest.raises(gate.GateError, match="NOT_EXECUTED"):
            gate.require_tasks(incomplete)
    with pytest.raises(gate.GateError, match="INVENTORY"):
        gate.require_tasks(tasks[:-1])
    with pytest.raises(gate.GateError, match="INVENTORY"):
        gate.require_tasks(tasks + [tasks[0]])


def test_rejected_candidate_requires_prior_execution_and_denied_downstream():
    tasks = [
        {"task_id": task, "state": "success"}
        for task in ("validate", "acquire", "silver", "warehouse")
    ] + [
        {"task_id": "candidate", "state": "failed"},
        {"task_id": "activate", "state": "upstream_failed"},
        {"task_id": "outcome", "state": "upstream_failed"},
    ]
    gate.require_tasks(tasks, "candidate")
    tasks[-2]["state"] = "success"
    with pytest.raises(gate.GateError, match="REJECTION_TASK_STATE"):
        gate.require_tasks(tasks, "candidate")


def test_fixture_parameters_keep_explicit_limits_and_fixed_baseline():
    baseline = {"silver_version_id": "a" * 64, "manifest_sha256": "b" * 64}
    value = gate.parameters("demo-b", baseline)
    assert value["baseline_version"] == baseline["silver_version_id"]
    assert value["baseline_sha256"] == baseline["manifest_sha256"]
    assert value["mode"] == "demo" and value["fixture_id"] == "demo-b"
    for name in (
        "total_rows",
        "total_requests",
        "grant_rows",
        "grant_requests",
        "page_size",
        "build_seconds",
        "storage_bytes",
    ):
        assert type(value[name]) is int and value[name] > 0


def test_pipeline_job_requires_real_gate_and_uploads_only_sanitized_summary():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["pipeline-e2e"]
    assert {"quality", "optional-runtimes"} <= set(job["needs"])
    assert 1 <= job["timeout-minutes"] <= 120
    commands = [step["run"] for step in job["steps"] if "run" in step]
    assert "make pipeline-e2e" in commands
    upload = [
        step for step in job["steps"] if step.get("name") == "Upload sanitized pipeline evidence"
    ][0]
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "reports/orchestration/e2e.json"
    assert upload["with"]["if-no-files-found"] == "error"


def test_runtime_recipe_hash_detects_changed_executing_source(tmp_path):
    package = tmp_path / "src/observatorio_secop"
    package.mkdir(parents=True)
    (tmp_path / "scripts").mkdir()
    for name in ("uv.lock", "pyproject.toml", "scripts/gold_dbt.py"):
        (tmp_path / name).write_text("fixed\n")
    source = package / "worker.py"
    source.write_text("original\n")
    before = gate.implementation_digest(tmp_path)
    source.write_text("changed\n")
    assert gate.implementation_digest(tmp_path) != before


def test_diagnostics_remain_private_and_bounded(tmp_path):
    probe = object.__new__(gate.Probe)
    probe.private = tmp_path
    probe.diagnostic_count = 0
    probe.save_diagnostic("failure", "PRIVATE_CANARY" + "x" * 1048576)
    path = tmp_path / "0001-failure.log"
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.stat().st_size == 1048576
    assert "PRIVATE_CANARY" in path.read_text()


def test_disposable_stack_has_bounded_memory_swap_and_process_counts(tmp_path):
    probe = object.__new__(gate.Probe)
    probe.project = "secop-e2e-test"
    probe.root = tmp_path
    probe.private = tmp_path / "private"
    probe.private.mkdir()
    probe.diagnostic_count = 0
    probe.deadline = 10**9
    probe.environment = {}
    probe.api = "http://127.0.0.1:8080"
    probe.token = None
    probe.task_outcomes = {}
    # Build the generated override via the same constructor, without starting services.
    gate.Probe.__init__(probe, tmp_path, 1200, tmp_path / "private")
    override = yaml.safe_load((tmp_path / "scheduler.json").read_text())
    services = override["services"]
    expected = {
        "postgres": 256,
        "airflow-api-server": 512,
        "airflow-scheduler": 1536,
        "airflow-dag-processor": 256,
    }
    expected_pids = {
        "postgres": 128,
        "airflow-api-server": 192,
        "airflow-scheduler": 384,
        "airflow-dag-processor": 128,
    }
    for service, memory in expected.items():
        assert services[service]["mem_limit"] == f"{memory}m"
        assert services[service]["memswap_limit"] == f"{memory}m"
        assert services[service]["pids_limit"] == expected_pids[service]
    assert sum(expected.values()) == 2560


@pytest.mark.parametrize(
    "contents,expected",
    [
        ("MemTotal: 4096000 kB\nMemAvailable: 3145728 kB\n", 3072),
        ("MemAvailable: 1000000 kB\n", 976),
        ("MemTotal: 1000 kB\n", 0),
    ],
)
def test_e2e_memory_preflight_reads_available_memory_and_fails_closed(tmp_path, contents, expected):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(contents)
    assert gate.available_memory_mib(meminfo) == expected


def test_e2e_memory_preflight_fails_closed_when_procfs_is_unavailable(tmp_path):
    assert gate.available_memory_mib(tmp_path / "missing") == 0
    assert gate.E2E_MIN_AVAILABLE_MIB == 2048


def test_rejected_candidate_is_explicitly_inspected_before_discard(monkeypatch):
    probe = object.__new__(gate.Probe)
    captured = []

    def command(args):
        captured.append(args)
        return '{"state":"discarded"}'

    monkeypatch.setattr(probe, "command", command)
    probe.discard_rejected("a" * 32)
    code = captured[0][-2]
    assert "row['state'] != 'failed'" in code
    assert "row['ever_activated']" in code
    assert "store.discard(sys.argv[1])" in code


def test_every_commit_boundary_retry_must_execute_exactly_twice():
    receipt = {
        "stages": {stage: {"attempt": 2, "status": "success"} for stage in gate.RECOVERY_STAGES},
        "invocations": 1,
        "acquisition_rows": 5,
    }
    gate.require_retries(receipt)
    for stage in gate.RECOVERY_STAGES:
        missing = {
            **receipt,
            "stages": {key: value for key, value in receipt["stages"].items() if key != stage},
        }
        with pytest.raises(gate.GateError, match="FIXED_INPUT_RETRY_FAILED"):
            gate.require_retries(missing)
        changed = {
            **receipt,
            "stages": receipt["stages"] | {stage: {"attempt": 1, "status": "success"}},
        }
        with pytest.raises(gate.GateError, match="FIXED_INPUT_RETRY_FAILED"):
            gate.require_retries(changed)
    with pytest.raises(gate.GateError, match="FIXED_INPUT_RETRY_FAILED"):
        gate.require_retries(receipt | {"invocations": 2})


def test_serialized_scheduler_evidence_requires_a_queued_run_without_effects():
    case = {
        "status": "passed",
        "queued_observations": 4,
        "second_task_instances_before_release": 7,
        "second_tasks_started_before_release": 0,
        "second_receipt_before_release": False,
        "second_validate_started_after_first_finished": True,
        "second_failed_validation_without_receipt": True,
        "publication_unchanged": True,
    }
    gate.require_serialized_run(case)
    for field, value in (
        ("queued_observations", 2),
        ("second_task_instances_before_release", -1),
        ("second_tasks_started_before_release", 1),
        ("second_receipt_before_release", True),
        ("second_validate_started_after_first_finished", False),
        ("second_failed_validation_without_receipt", False),
        ("publication_unchanged", False),
    ):
        with pytest.raises(gate.GateError, match="SERIALIZATION_NOT_ENFORCED"):
            gate.require_serialized_run(case | {field: value})
    with pytest.raises(gate.GateError, match="SERIALIZATION_NOT_ENFORCED"):
        gate.require_serialized_run(case | {"status": "skipped"})


def test_queued_run_task_instances_must_have_no_state_or_start_end_time():
    gate.require_unstarted_tasks(
        [{"task_id": "validate", "state": None, "start_date": None, "end_date": None}]
    )
    for task in (
        {"task_id": "validate", "state": "skipped", "start_date": None, "end_date": None},
        {"task_id": "validate", "state": "deferred", "start_date": None, "end_date": None},
        {"task_id": "validate", "state": "future-state", "start_date": None, "end_date": None},
        {
            "task_id": "validate",
            "state": None,
            "start_date": "2026-10-25T12:00:00Z",
            "end_date": None,
        },
        {
            "task_id": "validate",
            "state": None,
            "start_date": None,
            "end_date": "2026-10-25T12:00:01Z",
        },
        {"task_id": "validate", "start_date": None, "end_date": None},
    ):
        with pytest.raises(gate.GateError, match="SERIALIZATION_SECOND_TASK_STARTED"):
            gate.require_unstarted_tasks([task])


def test_exact_restoration_preserves_original_contents_with_one_new_event():
    original = {
        "publication_id": "a" * 32,
        "generation": 1,
        "events": 1,
        "rows": 2,
        "total": "300.00",
        "digest": "fact-a",
        "metadata_digest": "metadata-a",
        "quality_digest": "quality-a",
    }
    before = original | {"publication_id": "b" * 32, "generation": 2, "events": 2}
    restored = original | {"generation": 3, "events": 3}
    gate.require_restoration(original, before, restored)
    for field, value in (
        ("rows", 3),
        ("total", "400.00"),
        ("metadata_digest", "new"),
        ("quality_digest", "new"),
        ("generation", 4),
        ("events", 4),
    ):
        with pytest.raises(gate.GateError, match="RESTORATION_MISMATCH"):
            gate.require_restoration(original, before, restored | {field: value})
    assert "exact_restoration" in gate.REQUIRED_CASES


def test_restoration_uses_supported_pinned_operations(monkeypatch):
    probe = object.__new__(gate.Probe)
    captured = []

    def command(args):
        captured.append(args)
        return "{}"

    monkeypatch.setattr(probe, "command", command)
    probe.restore({"publication_id": "a" * 32}, {}, {"generation": 2})
    code = captured[0][-2]
    assert "manifest_sha256=bundle.manifest_sha256,expected_current=expected,restore=True" in code
    assert "WarehouseLoader(config).load(bundle,restore=True" in code
    assert "publication['generation'],restore=True" in code


def test_safe_surface_gate_rejects_missing_or_false_evidence():
    role = {"denied_operations": 4, "published_reads": 12, "owner_membership_denied": True}
    audit = {
        "airflow_logs_secret_free": True,
        "xcom_execution_ids_only": True,
        "xcom_rows_checked": 18,
        "log_files_checked": 1,
        "reader_acl_checks": [role, role],
    }
    gate.require_safe_surfaces(audit)
    for change in (
        {"airflow_logs_secret_free": False},
        {"xcom_execution_ids_only": False},
        {"xcom_rows_checked": 0},
        {"log_files_checked": 0},
        {"reader_acl_checks": []},
    ):
        with pytest.raises(gate.GateError, match="SAFE_SURFACE_CHECK_FAILED"):
            gate.require_safe_surfaces(audit | change)


def test_runtime_audit_code_is_valid_and_does_not_serialize_credentials():
    import runpy

    code = runpy.run_path(str(ROOT / "scripts/pipeline_e2e_checks.py"))["AUDIT_CODE"]
    compile(code, "pipeline-safe-surface-audit", "exec")
    # Credentials are read and compared inside the isolated process, not payload/export fields.
    assert "secrets=[os.environ['POSTGRES_PASSWORD']" in code
    output = code[code.rfind("print(json.dumps(") :]
    assert "secrets" not in output and "password" not in output
    assert "reader_acl_checks" in output
