"""End-to-end Bronze-to-Silver orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.logging import EventLogger
from observatorio_secop.processing.silver.bronze import (
    read_bronze_dataframe,
    resolve_bronze_runs,
)
from observatorio_secop.processing.silver.config import (
    SilverConfig,
    TerritoryCatalog,
    load_territory_catalog,
)
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.models import SilverManifest
from observatorio_secop.processing.silver.quality import validate_quality
from observatorio_secop.processing.silver.schemas import BRONZE_FIELDS
from observatorio_secop.processing.silver.storage import (
    SilverStorage,
    canonical_json,
    sha256_bytes,
)
from observatorio_secop.processing.silver.transform import transform_bronze

Clock = Callable[[], datetime]


class SilverPipeline:
    def __init__(
        self,
        config: SilverConfig,
        *,
        config_path: Path,
        spark: SparkSession | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        logger: EventLogger | None = None,
    ) -> None:
        self.config = config
        self.config_path = config_path
        self.catalog: TerritoryCatalog = load_territory_catalog(config.territory_catalog_path)
        self.contract = load_bronze_contract(config.contract_path, BRONZE_FIELDS)
        if self.contract.dataset_id != config.dataset_id:
            raise PublicationError("Silver config and source contract identify different datasets")
        self.config_sha256 = hashlib.sha256(config_path.read_bytes()).hexdigest()
        self.storage = SilverStorage(config.silver_root)
        self.clock = clock
        self.logger = logger or EventLogger()
        self._provided_spark = spark

    def run(self, run_ids: tuple[str, ...] = ()) -> SilverManifest:
        runs = resolve_bronze_runs(
            self.config.bronze_root,
            dataset_id=self.config.dataset_id,
            contract_sha256=self.contract.contract_hash,
            run_ids=run_ids,
        )
        version_id = self._version_id(runs)
        existing = self.storage.existing_manifest(version_id)
        if existing is not None:
            expected_identity = {
                "dataset_id": self.config.dataset_id,
                "transformation_version": self.config.transformation_version,
                "key_rule_version": self.config.key_rule_version,
                "contract_sha256": self.contract.contract_hash,
                "config_sha256": self.config_sha256,
                "territory_catalog_sha256": self.catalog.digest,
                "bronze_runs": [
                    {"run_id": run.run_id, "manifest_sha256": run.manifest_sha256} for run in runs
                ],
            }
            if any(existing.get(key) != value for key, value in expected_identity.items()):
                raise PublicationError(f"Silver version {version_id} has conflicting identity")
            self.storage.verify_existing(version_id, existing)
            self.storage.rollback(version_id)
            self.logger.emit("silver_idempotent", run_id=version_id, bronze_runs=len(runs))
            return SilverManifest.from_dict(existing)

        started = self._now()
        spark, owns_spark = self._spark()
        try:
            bronze = read_bronze_dataframe(spark, runs)
            transformed = transform_bronze(
                bronze, self.config, self.catalog, silver_version_id=version_id
            )
            results = validate_quality(
                spark,
                transformed.valid,
                transformed.counts,
                self.config,
                self.catalog,
            )
            manifest = SilverManifest(
                silver_version_id=version_id,
                dataset_id=self.config.dataset_id,
                transformation_version=self.config.transformation_version,
                key_rule_version=self.config.key_rule_version,
                contract_sha256=self.contract.contract_hash,
                config_sha256=self.config_sha256,
                territory_catalog_sha256=self.catalog.digest,
                bronze_runs=[
                    {
                        "run_id": run.run_id,
                        "manifest_sha256": run.manifest_sha256,
                    }
                    for run in runs
                ],
                counts=transformed.counts,
                quality_results=results,
                status="staging",
                started_at_utc=started,
                limitations=[
                    "The source does not declare a timezone for ultima_actualizacion.",
                    "When source_updated_at is null, Bronze observation lineage "
                    "breaks version ties.",
                ],
            )
            self.storage.create_staging(version_id)
            self.storage.write_data(
                transformed.valid,
                transformed.rejected,
                version_id,
                codec=self.config.parquet_codec,
                partitions=self.config.output_partitions,
            )
            self.storage.write_quality(version_id, manifest)
            manifest.files = self.storage.inventory(version_id)
            manifest.status = "valid" if manifest.critical_passed else "invalid"
            manifest.finished_at_utc = self._now()
            self.storage.write_manifest(version_id, manifest)
            self.storage.verify_staging(spark, manifest, codec=self.config.parquet_codec)
            if manifest.critical_passed:
                location = self.storage.promote(manifest)
                event = "silver_published"
            else:
                location = self.storage.invalidate(manifest)
                event = "silver_invalid"
            self.logger.emit(
                event,
                run_id=version_id,
                status=manifest.status,
                input_rows=manifest.counts["input_rows"],
                output_rows=manifest.counts["output_rows"],
                rejected_rows=manifest.counts["rejected_rows"],
                duplicate_rows=manifest.counts["duplicate_rows"],
                location=str(location),
            )
            return manifest
        finally:
            if owns_spark:
                spark.stop()

    def _version_id(self, runs: tuple[Any, ...]) -> str:
        identity = {
            "transformation_version": self.config.transformation_version,
            "dataset_id": self.config.dataset_id,
            "bronze_runs": [
                {"run_id": run.run_id, "manifest_sha256": run.manifest_sha256} for run in runs
            ],
            "contract_sha256": self.contract.contract_hash,
            "config_sha256": self.config_sha256,
            "territory_catalog_sha256": self.catalog.digest,
        }
        return sha256_bytes(canonical_json(identity))

    def _spark(self) -> tuple[SparkSession, bool]:
        if self._provided_spark is not None:
            self._configure_spark(self._provided_spark)
            return self._provided_spark, False
        spark = (
            SparkSession.builder.master(f"local[{self.config.local_threads}]")
            .appName("observatorio-secop-silver")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        self._configure_spark(spark)
        return spark, True

    def _configure_spark(self, spark: SparkSession) -> None:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        spark.conf.set("spark.sql.shuffle.partitions", str(self.config.local_threads))
        spark.sparkContext.setLogLevel("ERROR")

    def _now(self) -> str:
        return (
            self.clock().astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )


def manifest_summary(manifest: SilverManifest) -> str:
    return json.dumps(
        {
            "silver_version_id": manifest.silver_version_id,
            "status": manifest.status,
            "counts": manifest.counts,
            "critical_passed": manifest.critical_passed,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
