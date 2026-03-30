"""Run isolation and management for the satellite image dataset collection pipeline.

Each run receives a unique run_id and all outputs are written into that run's
isolated directory tree.
"""

from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.config.loader import freeze_config
from src.run.manifest import Manifest

logger = logging.getLogger(__name__)

VALID_STAGES = (
    "logs",
    "sampling",
    "scenes",
    "patches",
    "pools",
    "diagnostics",
    "qc",
)

VALID_STATUSES = ("started", "completed", "failed", "skipped")


def _generate_run_id() -> str:
    """Return a unique run identifier: ``YYYYMMDD_HHMMSS_<6-hex>``."""
    now = datetime.now(timezone.utc)
    hex_suffix = secrets.token_hex(3)  # 6 hex digits
    return f"{now.strftime('%Y%m%d_%H%M%S')}_{hex_suffix}"


class RunManager:
    """Manages a single pipeline run and its directory layout.

    Args:
        config: Validated configuration dictionary.
        output_root: Root directory under which all run directories are created.
    """

    def __init__(self, config: dict, output_root: str = "runs") -> None:
        self._run_id: str = _generate_run_id()
        self._run_dir: Path = Path(output_root) / self._run_id
        self._run_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        for stage in VALID_STAGES:
            (self._run_dir / stage).mkdir(exist_ok=True)

        # Snapshot configuration
        config_path = self._run_dir / "config_snapshot.yaml"
        self._config_hash: str = freeze_config(config, str(config_path))

        # Initialise manifest
        self._manifest = Manifest(self._run_id, self._config_hash, str(self._run_dir))

        logger.info("Run initialised: %s (dir=%s)", self._run_id, self._run_dir)

    # ---- properties --------------------------------------------------------

    @property
    def run_id(self) -> str:
        """Unique identifier for this run."""
        return self._run_id

    @property
    def run_dir(self) -> Path:
        """Root directory of this run."""
        return self._run_dir

    @property
    def config_hash(self) -> str:
        """SHA-256 hash of the frozen configuration snapshot."""
        return self._config_hash

    # ---- public methods ----------------------------------------------------

    def get_path(self, stage: str, filename: Optional[str] = None) -> Path:
        """Return the directory (or file) path for the given pipeline stage.

        Args:
            stage: One of the valid stage names.
            filename: Optional filename to append.

        Returns:
            A :class:`~pathlib.Path` to the stage directory or file.

        Raises:
            ValueError: If *stage* is not a recognised stage name.
        """
        if stage not in VALID_STAGES:
            raise ValueError(
                f"Invalid stage '{stage}'. Valid stages: {VALID_STAGES}"
            )
        path = self._run_dir / stage
        if filename is not None:
            path = path / filename
        return path

    def update_manifest(
        self,
        stage: str,
        status: str,
        metadata: Optional[dict] = None,
    ) -> None:
        """Record a stage transition in the run manifest.

        Args:
            stage: Pipeline stage name.
            status: One of ``"started"``, ``"completed"``, ``"failed"``,
                ``"skipped"``.
            metadata: Optional extra information to attach to the entry.

        Raises:
            ValueError: If *status* is not a valid status value.
        """
        if status not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status '{status}'. Valid values: {VALID_STATUSES}"
            )
        self._manifest.update_stage(stage, status, metadata)
        logger.info("Manifest updated: stage=%s status=%s", stage, status)

    def is_stage_completed(self, stage: str) -> bool:
        """Return ``True`` if the given stage is marked as completed.

        Args:
            stage: Pipeline stage name.
        """
        return self._manifest.get_stage_status(stage) == "completed"

    def can_resume(self, config_hash: str) -> bool:
        """Check whether it is safe to resume with the given config hash.

        Args:
            config_hash: SHA-256 hash to compare against.

        Returns:
            ``True`` if *config_hash* matches this run's config hash.
        """
        return config_hash == self._config_hash

    # ---- classmethod -------------------------------------------------------

    @classmethod
    def resume_run(cls, run_dir: str, config: dict) -> "RunManager":
        """Restore a :class:`RunManager` from an existing run directory.

        The method re-loads the manifest and verifies that the configuration
        hash matches before returning a reconstituted manager instance.

        Args:
            run_dir: Path to the existing run directory.
            config: Current configuration dictionary (used for hash comparison).

        Returns:
            A restored :class:`RunManager`.

        Raises:
            RuntimeError: If the config hash does not match the stored snapshot.
        """
        run_path = Path(run_dir)
        if not run_path.exists():
            raise FileNotFoundError(f"Run directory does not exist: {run_dir}")

        # Build a bare instance without running __init__
        instance = cls.__new__(cls)
        instance._run_dir = run_path
        instance._run_id = run_path.name

        # Re-load manifest
        manifest_path = run_path / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Manifest not found in {run_dir}"
            )
        instance._manifest = Manifest.load(str(manifest_path))

        # Recompute config hash and compare with stored value
        import hashlib
        import yaml

        serialized = yaml.dump(config, default_flow_style=False, allow_unicode=True)
        current_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        stored_hash = instance._manifest.to_dict().get("config_hash")

        if current_hash != stored_hash:
            raise RuntimeError(
                f"Config hash mismatch — cannot safely resume. "
                f"stored={stored_hash}, current={current_hash}"
            )

        instance._config_hash = stored_hash
        logger.info("Resumed run %s from %s", instance._run_id, run_dir)
        return instance
