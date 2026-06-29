"""Structured logging configuration for pipeline observability."""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def setup_logging(level: str = "INFO") -> structlog.BoundLogger:
    """Configure structlog with JSON output for production monitoring."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    return structlog.get_logger("fraud_detection")


def log_pipeline_event(
    logger: structlog.BoundLogger,
    event: str,
    layer: str,
    **kwargs: Any,
) -> None:
    """Emit a standardized pipeline event for monitoring dashboards."""
    logger.info(event, pipeline="fraud_detection", layer=layer, **kwargs)
