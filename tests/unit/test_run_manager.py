"""Unit tests for src.run.manager.RunManager."""

import json
import re
import sys
from pathlib import Path

import pytest

# Ensure project root is importable
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.run.manager import RunManager

_RUN_ID_PATTERN = re.compile(r"^\d{8}_\d{6}_[0-9a-f]{6}$")

EXPECTED_SUBDIRS = {"logs", "sampling", "scenes", "patches", "pools", "diagnostics", "qc"}


@pytest.fixture
def minimal_config():
    return {
        "aoi": {"name": "test"},
        "temporal": {"year": 2023},
        "sensor": {"collection": "test"},
        "sampling": {"grid_spacing_m": 5000},
        "scene_filter": {"max_cloud_percent": 20},
        "patch": {"size_pixels": 256},
        "quality": {"max_nodata_ratio": 0.0},
        "export": {"method": "compute_pixels"},
        "run": {"output_root": "", "allow_resume": True, "log_level": "INFO"},
        "gee": {"project": "test-project"},
    }


@pytest.fixture
def run_manager(minimal_config, tmp_path):
    return RunManager(minimal_config, output_root=str(tmp_path))


# ---- test cases ------------------------------------------------------------


def test_run_id_format(run_manager):
    assert _RUN_ID_PATTERN.match(run_manager.run_id), (
        f"run_id '{run_manager.run_id}' does not match YYYYMMDD_HHMMSS_<6hex>"
    )


def test_directory_creation(run_manager):
    for subdir in EXPECTED_SUBDIRS:
        assert (run_manager.run_dir / subdir).is_dir(), f"Missing subdirectory: {subdir}"


def test_config_snapshot_saved(run_manager):
    snapshot = run_manager.run_dir / "config_snapshot.yaml"
    assert snapshot.exists()
    assert snapshot.stat().st_size > 0


def test_manifest_initialized(run_manager):
    manifest_path = run_manager.run_dir / "manifest.json"
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert data["run_id"] == run_manager.run_id
    assert data["config_hash"] == run_manager.config_hash
    assert "stages" in data
    assert "summary" in data
    assert data["pipeline_version"] == "0.1.0"


def test_get_path_valid_stage(run_manager):
    path = run_manager.get_path("sampling")
    assert path == run_manager.run_dir / "sampling"

    path_with_file = run_manager.get_path("logs", "output.log")
    assert path_with_file == run_manager.run_dir / "logs" / "output.log"


def test_get_path_invalid_stage(run_manager):
    with pytest.raises(ValueError, match="Invalid stage"):
        run_manager.get_path("nonexistent_stage")


def test_update_manifest(run_manager):
    run_manager.update_manifest("sampling", "started")
    manifest_path = run_manager.run_dir / "manifest.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert data["stages"]["sampling"]["status"] == "started"


def test_is_stage_completed(run_manager):
    assert run_manager.is_stage_completed("sampling") is False
    run_manager.update_manifest("sampling", "completed")
    assert run_manager.is_stage_completed("sampling") is True


def test_resume_run_matching_hash(minimal_config, tmp_path):
    original = RunManager(minimal_config, output_root=str(tmp_path))
    resumed = RunManager.resume_run(str(original.run_dir), minimal_config)
    assert resumed.run_id == original.run_id
    assert resumed.config_hash == original.config_hash


def test_resume_run_mismatched_hash(minimal_config, tmp_path):
    original = RunManager(minimal_config, output_root=str(tmp_path))
    altered_config = {**minimal_config, "aoi": {"name": "different"}}
    with pytest.raises(RuntimeError, match="Config hash mismatch"):
        RunManager.resume_run(str(original.run_dir), altered_config)
