"""Configuration loader for the satellite image dataset collection pipeline.

Loads YAML-based base config with optional mode overlay, performs deep merge,
validates required keys, and snapshots the final config to disk.
"""

import hashlib
import logging
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

REQUIRED_TOP_LEVEL_KEYS = [
    "aoi",
    "temporal",
    "sensor",
    "sampling",
    "scene_filter",
    "patch",
    "quality",
    "export",
    "run",
]


def deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge *overlay* into *base* and return the result.

    * For nested dicts the merge recurses.
    * All other values (including lists) in *overlay* replace the
      corresponding value in *base*.

    Args:
        base: Base dictionary.
        overlay: Dictionary whose values take priority.

    Returns:
        A new merged dictionary.
    """
    merged = base.copy()
    for key, value in overlay.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(base_path: str, overlay_path: Optional[str] = None) -> dict:
    """Load a YAML base config and optionally deep-merge a mode overlay.

    Args:
        base_path: Path to the base YAML configuration file.
        overlay_path: Optional path to an overlay YAML file whose values
            override the base config.

    Returns:
        The (merged) configuration dictionary.

    Raises:
        FileNotFoundError: If a config file does not exist.
        yaml.YAMLError: If a file contains invalid YAML.
    """
    with open(base_path, encoding="utf-8") as f:
        base = yaml.safe_load(f) or {}

    if overlay_path is not None:
        with open(overlay_path, encoding="utf-8") as f:
            overlay = yaml.safe_load(f) or {}
        base = deep_merge(base, overlay)

    return base


def validate_config(config: dict) -> None:
    """Validate that all required top-level keys are present.

    Args:
        config: Configuration dictionary to validate.

    Raises:
        ValueError: If one or more required keys are missing.
    """
    missing = [key for key in REQUIRED_TOP_LEVEL_KEYS if key not in config]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")


def freeze_config(config: dict, output_path: str) -> str:
    """Snapshot the configuration to a YAML file and return its SHA-256 hash.

    Args:
        config: Configuration dictionary to serialize.
        output_path: Destination file path for the YAML snapshot.

    Returns:
        Hex-encoded SHA-256 hash of the serialized YAML content.
    """
    serialized = yaml.dump(config, default_flow_style=False, allow_unicode=True)
    sha256_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(serialized)

    logger.info("Config snapshot saved to %s (SHA-256: %s)", output_path, sha256_hash)
    return sha256_hash
