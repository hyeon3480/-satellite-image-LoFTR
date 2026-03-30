"""Main entry point for the satellite image dataset collection pipeline.

Phase A skeleton — verifies that the project scaffolding (config loading,
run management, logging, and GEE initialisation) works end-to-end.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path so `src.*` imports resolve.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config.loader import load_config
from src.run.manager import RunManager
from src.utils.log_setup import setup_logging

logger = logging.getLogger(__name__)

PIPELINE_STAGES = [
    "sampling",
    "scene_search",
    "patch_generation",
    "quality_assessment",
    "export",
    "packaging",
    "diagnostics",
]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Satellite Image Dataset Collection Pipeline",
    )
    parser.add_argument(
        "--base-config",
        type=str,
        default="configs/base.yaml",
        help="Path to base YAML configuration (default: configs/base.yaml)",
    )
    parser.add_argument(
        "--mode-config",
        type=str,
        default=None,
        help="Path to mode overlay YAML configuration (optional)",
    )
    parser.add_argument(
        "--resume-run",
        type=str,
        default=None,
        help="Path to existing run directory to resume (optional)",
    )
    return parser.parse_args()


def init_gee(project: str) -> None:
    """Initialise Google Earth Engine.

    Args:
        project: GEE cloud project identifier.
    """
    try:
        import ee
        ee.Initialize(project=project)
        logger.info("GEE initialised (project=%s)", project)
    except Exception as exc:
        logger.error("GEE initialisation failed: %s", exc)
        sys.exit(1)


def run_pipeline(args: argparse.Namespace) -> None:
    """Execute the full pipeline based on parsed CLI arguments.

    Args:
        args: Parsed command-line arguments.
    """
    # --- Load configuration -------------------------------------------------
    config = load_config(args.base_config, args.mode_config)

    # --- Initialise or resume run -------------------------------------------
    resuming = args.resume_run is not None
    if resuming:
        run_mgr = RunManager.resume_run(args.resume_run, config)
        logger.info("Resuming run %s", run_mgr.run_id)
    else:
        output_root = config.get("run", {}).get("output_root", "runs")
        run_mgr = RunManager(config, output_root=output_root)

    # --- Setup logging (file + console) -------------------------------------
    log_level = config.get("run", {}).get("log_level", "INFO")
    log_dir = str(run_mgr.get_path("logs"))
    setup_logging(log_dir, run_mgr.run_id, level=log_level)

    logger.info("Run ID  : %s", run_mgr.run_id)
    logger.info("Run dir : %s", run_mgr.run_dir)

    # --- Initialise GEE -----------------------------------------------------
    gee_project = config.get("gee", {}).get("project")
    if gee_project:
        init_gee(gee_project)
    else:
        logger.warning("No GEE project specified in config — skipping GEE init")

    # --- Execute pipeline stages --------------------------------------------
    for stage in PIPELINE_STAGES:
        if resuming and run_mgr.is_stage_completed(stage):
            logger.info("[%s] already completed — skipping (resume mode)", stage)
            continue

        run_mgr.update_manifest(stage, "started")
        logger.info("[%s] scheduled (not yet implemented)", stage)
        run_mgr.update_manifest(stage, "completed")

    # --- Done ---------------------------------------------------------------
    logger.info(
        "Pipeline finished — run_id=%s, run_dir=%s",
        run_mgr.run_id,
        run_mgr.run_dir,
    )


if __name__ == "__main__":
    run_pipeline(parse_args())
