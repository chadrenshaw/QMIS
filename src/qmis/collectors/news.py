"""News collector placeholder for QMIS."""

from __future__ import annotations


NEWS_COLLECTOR_STATUS = {
    "status": "blocked",
    "configured": False,
    "reason": (
        "The QMIS news collector is intentionally blocked because no source/provider is specified "
        "in the authoritative docs."
    ),
}


class NewsCollectorNotConfiguredError(RuntimeError):
    """Raised when the news collector is invoked before a provider is defined."""


def run_news_collector() -> int:
    """Placeholder entrypoint for the spec-required news collector module."""
    raise NewsCollectorNotConfiguredError(
        "The QMIS news collector cannot run yet because no news source/provider is specified "
        "in the authoritative docs."
    )
