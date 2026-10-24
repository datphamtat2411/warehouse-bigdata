"""Progress and elapsed-time diagnostics for read-only publication validation."""

import json
import logging
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from time import perf_counter
from typing import Literal

_LOGGER = logging.getLogger(__name__)
_PHASES: ContextVar[tuple[dict[str, Counter[str]], ...]] = ContextVar(
    "validation_phases", default=()
)


def record_validation_io(transport: Literal["hadoop_cli", "webhdfs"], operation: str) -> None:
    """Attribute CLI starts or HTTP dispatch attempts to active phases, inclusively.

    These diagnostics never store validation results. Context-local state is discarded
    when a phase exits; nested totals must not be summed with their parents.
    """
    for counts in _PHASES.get():
        counts[transport][operation] += 1


@contextmanager
def validation_phase(phase):
    """Emit start/end, wall time and inclusive per-transport operation counts."""
    started = perf_counter()
    counts: dict[str, Counter[str]] = {"hadoop_cli": Counter(), "webhdfs": Counter()}
    token = _PHASES.set((*_PHASES.get(), counts))
    _LOGGER.info("validation_phase=%s status=STARTED", phase)
    status = "FAILED"
    try:
        yield
        status = "PASSED"
    finally:
        _PHASES.reset(token)
        _LOGGER.info(
            "validation_phase=%s status=%s elapsed_seconds=%.3f counts=inclusive "
            "hadoop_cli_subprocess_count=%d webhdfs_request_count=%d "
            "hadoop_cli_operations=%s webhdfs_operations=%s",
            phase,
            status,
            perf_counter() - started,
            sum(counts["hadoop_cli"].values()),
            sum(counts["webhdfs"].values()),
            json.dumps(counts["hadoop_cli"], sort_keys=True, separators=(",", ":")),
            json.dumps(counts["webhdfs"], sort_keys=True, separators=(",", ":")),
        )
