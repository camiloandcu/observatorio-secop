"""Explicit source-byte/projection registry; no blanket historical hash acceptance."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.identity import CONTRACTS_ROOT
from observatorio_secop.ingestion.errors import SourceContractError


@dataclass(frozen=True)
class SourceProfile:
    revision: str
    contract_sha256: str
    projection_version: str
    projection: tuple[str, ...]
    has_identity: bool
    contract_path: Path


class CompatibilityRegistry:
    def __init__(self, path: Path = CONTRACTS_ROOT / "secop_compatibility.yaml") -> None:
        try:
            content = path.read_bytes()
            raw = yaml.safe_load(content)
            self.digest = hashlib.sha256(content).hexdigest()
            self.profiles: list[SourceProfile] = []
            if raw["schema_version"] != 1 or raw["dataset_id"] != "jbjy-vk9h":
                raise ValueError("Unsupported compatibility registry")
            for entry in raw["profiles"]:
                snapshot = (path.parent / entry["contract_path"]).resolve()
                if not snapshot.is_relative_to(path.parent.resolve()):
                    raise ValueError("Contract snapshot escapes registry directory")
                if hashlib.sha256(snapshot.read_bytes()).hexdigest() != entry["contract_sha256"]:
                    raise ValueError("Contract revision snapshot hash mismatch")
                identity = yaml.safe_load(snapshot.read_bytes()).get("platform_identity")
                if (
                    identity
                    and hashlib.sha256(
                        (path.parent / identity["rules_path"]).read_bytes()
                    ).hexdigest()
                    != identity["rules_sha256"]
                ):
                    raise ValueError("Identity rules hash differs from supported source revision")
                self.profiles.append(
                    SourceProfile(
                        revision=entry["revision"],
                        contract_sha256=entry["contract_sha256"],
                        projection_version=entry["projection_version"],
                        projection=tuple(entry["projection"]),
                        has_identity=entry["has_identity"],
                        contract_path=snapshot,
                    )
                )
            combinations = {(p.contract_sha256, p.projection) for p in self.profiles}
            if len(combinations) != len(self.profiles):
                raise ValueError("Duplicated compatibility entries")
        except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError) as error:
            raise SourceContractError(f"Invalid source compatibility registry: {error}") from error

    def resolve(
        self,
        dataset: str,
        contract_hash: str,
        projection: Any,
        version: str | None = None,
        revision: str | None = None,
    ) -> SourceProfile:
        if dataset != "jbjy-vk9h" or not isinstance(projection, list):
            raise SourceContractError(
                "Source compatibility requires dataset and explicit projection"
            )
        for profile in self.profiles:
            if (
                profile.contract_sha256 == contract_hash
                and list(profile.projection) == projection
                and version in (None, profile.projection_version)
                and revision in (None, profile.revision)
            ):
                return profile
        raise SourceContractError("Unsupported source contract/projection/revision combination")
