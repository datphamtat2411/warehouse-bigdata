"""Progress and elapsed-time diagnostics for read-only publication validation."""

import logging
from contextlib import contextmanager
from time import perf_counter

_LOGGER = logging.getLogger(__name__)


@contextmanager
def validation_phase(phase):
    """Emit progress before work so a timeout retains the active phase."""
    started = perf_counter()
    _LOGGER.info("validation_phase=%s status=STARTED", phase)
    status = "FAILED"
    try:
        yield
        status = "PASSED"
    finally:
        _LOGGER.info(
            "validation_phase=%s status=%s elapsed_seconds=%.3f",
            phase,
            status,
            perf_counter() - started,
        )
