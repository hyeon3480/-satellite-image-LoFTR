"""Run manifest management for the satellite image dataset collection pipeline.

Tracks run metadata, per-stage status, and provenance information.  The
manifest is persisted as JSON and every mutation is flushed to disk
immediately using an atomic write pattern for crash safety.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_PIPELINE_VERSION = "0.1.0"


class Manifest:
    """Persistent metadata record for a single pipeline run.

    Args:
        run_id: Unique run identifier.
        config_hash: SHA-256 hash of the frozen configuration.
        run_dir: Path to the run directory where ``manifest.json`` is stored.
    """

    def __init__(self, run_id: str, config_hash: str, run_dir: str) -> None:
        self._path: Path = Path(run_dir) / "manifest.json"
        now = datetime.now(timezone.utc).isoformat()
        self._data: dict = {
            "run_id": run_id,
            "config_hash": config_hash,
            "created_at": now,
            "updated_at": now,
            "pipeline_version": _PIPELINE_VERSION,
            "stages": {},
            "summary": {},
        }
        self._save()
        logger.info("Manifest created at %s", self._path)

    # ---- public methods ----------------------------------------------------

    def update_stage(
        self,
        stage: str,
        status: str,
        metadata: Optional[dict] = None,
    ) -> None:
        """Record or update the status of a pipeline stage.

        Args:
            stage: Pipeline stage name.
            status: Current status string (e.g. ``"started"``,
                ``"completed"``, ``"failed"``, ``"skipped"``).
            metadata: Optional additional information to store alongside
                the stage record.
        """
        entry: dict = {
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if metadata:
            entry.update(metadata)
        self._data["stages"][stage] = entry
        self._data["updated_at"] = entry["updated_at"]
        self._save()
        logger.info("Stage '%s' -> %s", stage, status)

    def get_stage_status(self, stage: str) -> Optional[str]:
        """Return the current status of *stage*, or ``None`` if unrecorded.

        Args:
            stage: Pipeline stage name.

        Returns:
            Status string or ``None``.
        """
        entry = self._data["stages"].get(stage)
        if entry is None:
            return None
        return entry.get("status")

    def set_summary(self, key: str, value: Any) -> None:
        """Store a summary entry and persist to disk.

        Args:
            key: Summary field name.
            value: Arbitrary JSON-serialisable value.
        """
        self._data["summary"][key] = value
        self._data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._save()

    def to_dict(self) -> dict:
        """Return the full manifest as a plain dictionary.

        Returns:
            A copy of the internal manifest data.
        """
        return dict(self._data)

    # ---- classmethod -------------------------------------------------------

    @classmethod
    def load(cls, manifest_path: str) -> "Manifest":
        """Restore a :class:`Manifest` from an existing JSON file.

        Args:
            manifest_path: Path to the ``manifest.json`` file.

        Returns:
            A reconstituted :class:`Manifest` instance.

        Raises:
            FileNotFoundError: If *manifest_path* does not exist.
        """
        path = Path(manifest_path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        instance = cls.__new__(cls)
        instance._path = path
        instance._data = data
        logger.info("Manifest loaded from %s", path)
        return instance

    # ---- internal helpers --------------------------------------------------

    def _save(self) -> None:
        """Atomically write the manifest to disk.

        Writes to a temporary file in the same directory, then renames it
        over the target path so that a crash mid-write never leaves a
        corrupt ``manifest.json``.
        """
        dir_path = self._path.parent
        dir_path.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            suffix=".tmp", prefix="manifest_", dir=str(dir_path)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self._path)
        except BaseException:
            # Clean up the temp file on any failure
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
