"""Transactional checkpoint and logical observation index."""

from __future__ import annotations

import fcntl
import json
import sqlite3
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.errors import ConcurrentRunError
from observatorio_secop.ingestion.models import RunManifest
from observatorio_secop.ingestion.storage import canonical_json, sha256_bytes

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Observation:
    identity: str
    payload_hash: str
    page_path: str
    position: int


@dataclass(frozen=True)
class CommitCounts:
    new: int
    repeated: int
    new_versions: int


class DatasetLock(AbstractContextManager["DatasetLock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream: Any = None

    def __enter__(self) -> DatasetLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self.path.open("a+")
        try:
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._stream.close()
            raise ConcurrentRunError("Another ingestion process holds the dataset lock") from error
        return self

    def __exit__(self, *args: object) -> None:
        if self._stream:
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            self._stream.close()


class IngestionState:
    def __init__(self, path: Path) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                version INTEGER PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                dataset_id TEXT NOT NULL,
                lane TEXT NOT NULL,
                completed_through TEXT NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (dataset_id, lane)
            );
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                parameter_hash TEXT NOT NULL,
                manifest_path TEXT NOT NULL,
                committed_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS observations (
                dataset_id TEXT NOT NULL,
                identity_value TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                first_run_id TEXT NOT NULL,
                last_run_id TEXT NOT NULL,
                page_path TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (dataset_id, identity_value, payload_sha256)
            );
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_meta(version) VALUES (?)", (SCHEMA_VERSION,)
        )
        connection.commit()
        return connection

    def checkpoint(self, dataset_id: str, lane: str = "primary") -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT completed_through FROM checkpoints WHERE dataset_id=? AND lane=?",
                (dataset_id, lane),
            ).fetchone()
        return row[0] if row else None

    def has_run(self, run_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return row is not None

    def summary(self, dataset_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            checkpoint = connection.execute(
                "SELECT completed_through, run_id FROM checkpoints "
                "WHERE dataset_id=? AND lane='primary'",
                (dataset_id,),
            ).fetchone()
            run_count = connection.execute(
                "SELECT COUNT(*) FROM runs WHERE dataset_id=?", (dataset_id,)
            ).fetchone()[0]
            observation_count = connection.execute(
                "SELECT COUNT(*) FROM observations WHERE dataset_id=?", (dataset_id,)
            ).fetchone()[0]
        return {
            "checkpoint": checkpoint[0] if checkpoint else None,
            "checkpoint_run_id": checkpoint[1] if checkpoint else None,
            "committed_runs": run_count,
            "logical_observations": observation_count,
        }

    def commit(
        self,
        manifest: RunManifest,
        observations: list[Observation],
        *,
        manifest_path: str,
    ) -> CommitCounts:
        parameter_hash = sha256_bytes(canonical_json(manifest.parameters))
        new = repeated = new_versions = 0
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM runs WHERE run_id=?", (manifest.run_id,)
            ).fetchone():
                connection.rollback()
                return CommitCounts(0, len(observations), 0)
            for item in observations:
                existing = connection.execute(
                    "SELECT 1 FROM observations WHERE dataset_id=? AND identity_value=? "
                    "AND payload_sha256=?",
                    (manifest.dataset_id, item.identity, item.payload_hash),
                ).fetchone()
                prior_identity = connection.execute(
                    "SELECT 1 FROM observations WHERE dataset_id=? AND identity_value=? LIMIT 1",
                    (manifest.dataset_id, item.identity),
                ).fetchone()
                if existing:
                    repeated += 1
                    connection.execute(
                        "UPDATE observations SET last_run_id=? WHERE dataset_id=? "
                        "AND identity_value=? AND payload_sha256=?",
                        (
                            manifest.run_id,
                            manifest.dataset_id,
                            item.identity,
                            item.payload_hash,
                        ),
                    )
                else:
                    new += 1
                    new_versions += int(prior_identity is not None)
                    connection.execute(
                        "INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            manifest.dataset_id,
                            item.identity,
                            item.payload_hash,
                            manifest.run_id,
                            manifest.run_id,
                            item.page_path,
                            item.position,
                        ),
                    )
            connection.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?)",
                (
                    manifest.run_id,
                    manifest.dataset_id,
                    parameter_hash,
                    manifest_path,
                    manifest.finished_at_utc,
                ),
            )
            if manifest.range_complete and manifest.checkpoint_candidate:
                connection.execute(
                    "INSERT INTO checkpoints VALUES (?, 'primary', ?, ?) "
                    "ON CONFLICT(dataset_id, lane) DO UPDATE SET "
                    "completed_through=excluded.completed_through, run_id=excluded.run_id",
                    (manifest.dataset_id, manifest.checkpoint_candidate, manifest.run_id),
                )
        return CommitCounts(new, repeated, new_versions)


def observations_from_pages(
    run_directory: Path, manifest: RunManifest, identity_field: str
) -> list[Observation]:
    result = []
    for page in manifest.pages:
        rows = json.loads((run_directory / page.path).read_bytes())
        for position, row in enumerate(rows):
            result.append(
                Observation(
                    identity=str(row[identity_field]),
                    payload_hash=sha256_bytes(canonical_json(row)),
                    page_path=page.path,
                    position=position,
                )
            )
    return result
