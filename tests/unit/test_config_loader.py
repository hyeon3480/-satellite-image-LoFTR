"""Unit tests for src.config.loader."""

import sys
from pathlib import Path

import pytest
import yaml

# Ensure project root is importable
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config.loader import deep_merge, freeze_config, load_config, validate_config


# ---- deep_merge ------------------------------------------------------------

def test_deep_merge_basic():
    base = {"a": 1, "b": 2}
    overlay = {"c": 3, "d": 4}
    result = deep_merge(base, overlay)
    assert result == {"a": 1, "b": 2, "c": 3, "d": 4}


def test_deep_merge_nested():
    base = {"sensor": {"bands": ["B4"], "resolution": 10}}
    overlay = {"sensor": {"scale_factor": 0.0001}}
    result = deep_merge(base, overlay)
    assert result == {
        "sensor": {"bands": ["B4"], "resolution": 10, "scale_factor": 0.0001}
    }


def test_deep_merge_list_override():
    base = {"bands": ["B4", "B3", "B2"]}
    overlay = {"bands": ["B8"]}
    result = deep_merge(base, overlay)
    assert result["bands"] == ["B8"]


def test_deep_merge_overlay_priority():
    base = {"a": 1, "b": "old"}
    overlay = {"b": "new"}
    result = deep_merge(base, overlay)
    assert result["b"] == "new"


# ---- load_config -----------------------------------------------------------

def test_load_config_base_only(tmp_path: Path):
    base_file = tmp_path / "base.yaml"
    base_file.write_text(
        yaml.dump({"aoi": {"name": "test"}, "sensor": {"res": 10}}),
        encoding="utf-8",
    )
    config = load_config(str(base_file))
    assert config["aoi"]["name"] == "test"
    assert config["sensor"]["res"] == 10


def test_load_config_with_overlay(tmp_path: Path):
    base_file = tmp_path / "base.yaml"
    overlay_file = tmp_path / "overlay.yaml"
    base_file.write_text(
        yaml.dump({"aoi": {"name": "base_name", "bbox": [1, 2]}, "x": 1}),
        encoding="utf-8",
    )
    overlay_file.write_text(
        yaml.dump({"aoi": {"name": "overlay_name"}}),
        encoding="utf-8",
    )
    config = load_config(str(base_file), str(overlay_file))
    assert config["aoi"]["name"] == "overlay_name"
    assert config["aoi"]["bbox"] == [1, 2]
    assert config["x"] == 1


# ---- validate_config -------------------------------------------------------

def test_validate_config_valid():
    config = {
        "aoi": {},
        "temporal": {},
        "sensor": {},
        "sampling": {},
        "scene_filter": {},
        "patch": {},
        "quality": {},
        "export": {},
        "run": {},
    }
    validate_config(config)  # should not raise


def test_validate_config_missing_keys():
    config = {"aoi": {}, "sensor": {}}
    with pytest.raises(ValueError, match="Missing required config keys"):
        validate_config(config)


# ---- freeze_config ---------------------------------------------------------

def test_freeze_config_saves_file(tmp_path: Path):
    config = {"aoi": {"name": "test"}}
    out = tmp_path / "snapshot.yaml"
    freeze_config(config, str(out))
    assert out.exists()
    loaded = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert loaded == config


def test_freeze_config_returns_hash(tmp_path: Path):
    config = {"aoi": {"name": "test"}}
    out = tmp_path / "snapshot.yaml"
    result = freeze_config(config, str(out))
    assert isinstance(result, str)
    assert len(result) == 64  # SHA-256 hex digest length


def test_freeze_config_deterministic(tmp_path: Path):
    config = {"a": 1, "b": [2, 3], "c": {"d": 4}}
    hash1 = freeze_config(config, str(tmp_path / "snap1.yaml"))
    hash2 = freeze_config(config, str(tmp_path / "snap2.yaml"))
    assert hash1 == hash2
