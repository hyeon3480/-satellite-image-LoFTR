"""Unified logging configuration for the satellite image dataset collection pipeline.

Provides a single ``setup_logging`` entry-point that wires console *and* file
handlers to the root logger, plus a thin ``get_logger`` helper for per-module
loggers.
"""

import logging
from pathlib import Path


def setup_logging(
    log_dir: str,
    run_id: str,
    level: str = "INFO",
) -> logging.Logger:
    """Configure the root logger with console and file handlers.

    Args:
        log_dir: Directory where the log file will be written.
        run_id: Run identifier used as the log file name stem.
        level: Logging level as a string (``"DEBUG"``, ``"INFO"``,
            ``"WARNING"``, ``"ERROR"``).

    Returns:
        The configured root :class:`logging.Logger`.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Avoid duplicate handlers on repeated calls
    root_logger.handlers.clear()

    # Console handler — concise format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_fmt = logging.Formatter("[{levelname}] {message}", style="{")
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    # File handler — detailed format
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(
        log_path / f"{run_id}.log",
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_fmt = logging.Formatter(
        "{asctime} [{levelname}] {name} - {message}",
        style="{",
    )
    file_handler.setFormatter(file_fmt)
    root_logger.addHandler(file_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Return a module-specific logger.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` instance.
    """
    return logging.getLogger(name)
