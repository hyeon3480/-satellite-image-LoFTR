"""Per-stage diagnostics logger for the satellite image dataset collection pipeline.

Collects processing statistics, accept/reject counts, rejection reason codes,
and distributions for each pipeline stage.  Generates automatic warnings when
anomalous patterns are detected.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class StageLogger:
    """Accumulates and persists diagnostics for a single pipeline stage.

    Args:
        stage_name: Identifier for the stage (e.g. ``"sampling"``).
        output_dir: Directory where diagnostics files are written.
    """

    def __init__(self, stage_name: str, output_dir: str) -> None:
        self._stage_name: str = stage_name
        self._output_dir: Path = Path(output_dir)

        self._processed: int = 0
        self._accepted: int = 0
        self._rejected: int = 0
        self._skipped: int = 0
        self._errors: int = 0

        self._rejection_reasons: Counter[str] = Counter()
        self._distributions: dict[str, dict[str, int]] = {}

    # ---- accumulation methods ----------------------------------------------

    def log_processed(self, count: int = 1) -> None:
        """Accumulate total processed items.

        Args:
            count: Number of items processed.
        """
        self._processed += count

    def log_accepted(self, count: int = 1, metadata: Optional[dict] = None) -> None:
        """Accumulate accepted items.

        Args:
            count: Number of items accepted.
            metadata: Optional extra information (logged, not stored in counters).
        """
        self._accepted += count
        if metadata:
            logger.debug(
                "[%s] accepted +%d metadata=%s",
                self._stage_name, count, metadata,
            )

    def log_rejected(
        self,
        reason_code: str,
        count: int = 1,
        metadata: Optional[dict] = None,
    ) -> None:
        """Accumulate rejected items under a specific reason code.

        Args:
            reason_code: Machine-readable rejection reason
                (e.g. ``"CLOUD_FAIL"``).
            count: Number of items rejected.
            metadata: Optional extra information.
        """
        self._rejected += count
        self._rejection_reasons[reason_code] += count
        if metadata:
            logger.debug(
                "[%s] rejected +%d reason=%s metadata=%s",
                self._stage_name, count, reason_code, metadata,
            )

    def log_skipped(self, reason: str, count: int = 1) -> None:
        """Accumulate skipped items.

        Args:
            reason: Human-readable skip reason.
            count: Number of items skipped.
        """
        self._skipped += count
        logger.debug("[%s] skipped +%d reason=%s", self._stage_name, count, reason)

    def log_error(self, error_msg: str, count: int = 1) -> None:
        """Accumulate error items.

        Args:
            error_msg: Description of the error.
            count: Number of items that errored.
        """
        self._errors += count
        logger.warning("[%s] error +%d: %s", self._stage_name, count, error_msg)

    def log_distribution(self, dimension: str, distribution: dict[str, int]) -> None:
        """Record a categorical distribution for a given dimension.

        Args:
            dimension: Name of the dimension (e.g. ``"class"``).
            distribution: Mapping of category names to counts.
        """
        self._distributions[dimension] = dict(distribution)

    # ---- summary -----------------------------------------------------------

    def get_summary(self) -> dict[str, Any]:
        """Return all collected statistics as a dictionary.

        Returns:
            Dictionary containing totals, rejection reasons, distributions,
            and automatically generated warnings.
        """
        return {
            "stage_name": self._stage_name,
            "total_processed": self._processed,
            "total_accepted": self._accepted,
            "total_rejected": self._rejected,
            "total_skipped": self._skipped,
            "total_errors": self._errors,
            "rejection_reasons": dict(self._rejection_reasons),
            "distributions": self._distributions,
            "warnings": self._generate_warnings(),
        }

    # ---- persistence -------------------------------------------------------

    def save(self) -> str:
        """Persist the diagnostics summary to JSON and plain-text files.

        Files are written to ``{output_dir}/{stage_name}_diagnostics.json``
        and ``{output_dir}/{stage_name}_diagnostics.txt``.

        Returns:
            Path to the saved JSON file.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)

        summary = self.get_summary()

        # JSON output
        json_path = self._output_dir / f"{self._stage_name}_diagnostics.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # Human-readable text output
        txt_path = self._output_dir / f"{self._stage_name}_diagnostics.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self._format_text(summary))

        logger.info("Diagnostics saved to %s", json_path)
        return str(json_path)

    # ---- internal helpers --------------------------------------------------

    def _generate_warnings(self) -> list[str]:
        """Auto-generate warnings based on collected statistics."""
        warnings: list[str] = []

        # Low acceptance rate
        if self._processed > 0:
            acceptance_rate = self._accepted / self._processed
            if acceptance_rate < 0.10:
                warnings.append(
                    f"Low acceptance rate: {acceptance_rate:.1%} "
                    f"({self._accepted}/{self._processed})"
                )

        # Dominant single rejection reason
        if self._rejected > 0:
            for reason, count in self._rejection_reasons.items():
                if count / self._rejected > 0.50:
                    warnings.append(
                        f"Dominant rejection reason '{reason}': "
                        f"{count}/{self._rejected} "
                        f"({count / self._rejected:.1%})"
                    )

        # Distribution collapse (zero-count category)
        for dimension, dist in self._distributions.items():
            for category, count in dist.items():
                if count == 0:
                    warnings.append(
                        f"Distribution collapse in '{dimension}': "
                        f"category '{category}' has count 0"
                    )

        return warnings

    @staticmethod
    def _format_text(summary: dict[str, Any]) -> str:
        """Render a human-readable text report from a summary dict."""
        lines: list[str] = []
        lines.append(f"=== Stage: {summary['stage_name']} ===")
        lines.append(f"Processed : {summary['total_processed']}")
        lines.append(f"Accepted  : {summary['total_accepted']}")
        lines.append(f"Rejected  : {summary['total_rejected']}")
        lines.append(f"Skipped   : {summary['total_skipped']}")
        lines.append(f"Errors    : {summary['total_errors']}")

        if summary["rejection_reasons"]:
            lines.append("")
            lines.append("Rejection reasons:")
            for reason, count in sorted(
                summary["rejection_reasons"].items(),
                key=lambda x: x[1],
                reverse=True,
            ):
                lines.append(f"  {reason}: {count}")

        if summary["distributions"]:
            lines.append("")
            lines.append("Distributions:")
            for dim, dist in summary["distributions"].items():
                lines.append(f"  [{dim}]")
                for cat, count in sorted(
                    dist.items(), key=lambda x: x[1], reverse=True
                ):
                    lines.append(f"    {cat}: {count}")

        if summary["warnings"]:
            lines.append("")
            lines.append("Warnings:")
            for w in summary["warnings"]:
                lines.append(f"  ⚠ {w}")

        lines.append("")
        return "\n".join(lines)
