"""Schedule definitions for cron/systemd-friendly QMIS jobs."""

from __future__ import annotations

from pathlib import Path


COLLECTOR_GROUPS: dict[str, tuple[str, ...]] = {
    "market-15m": ("market", "crypto", "breadth"),
    "daily": ("macro", "liquidity", "solar", "astronomy", "natural"),
}

ANALYSIS_GROUPS: dict[str, tuple[str, ...]] = {
    "daily": ("features", "regime", "relationships", "lead-lag"),
}

ALERT_GROUPS: dict[str, tuple[str, ...]] = {
    "daily": ("alerts",),
}


def build_schedule_manifest(repo_root: Path) -> dict[str, list[dict[str, str]]]:
    """Return the recommended cron/systemd job manifest for QMIS."""
    python_prefix = "uv run python"
    return {
        "collectors": [
            {
                "cadence": cadence,
                "job": f"collectors:{cadence}",
                "command": f"{python_prefix} {repo_root / 'scripts' / 'run_collectors.py'} --cadence {cadence}",
                "group": ", ".join(group),
            }
            for cadence, group in COLLECTOR_GROUPS.items()
        ],
        "analysis": [
            {
                "cadence": cadence,
                "job": f"analysis:{cadence}",
                "command": f"{python_prefix} {repo_root / 'scripts' / 'run_analysis.py'} --cadence {cadence}",
                "group": ", ".join(group),
            }
            for cadence, group in ANALYSIS_GROUPS.items()
        ],
        "alerts": [
            {
                "cadence": cadence,
                "job": f"alerts:{cadence}",
                "command": f"{python_prefix} {repo_root / 'scripts' / 'run_alerts.py'} --cadence {cadence}",
                "group": ", ".join(group),
            }
            for cadence, group in ALERT_GROUPS.items()
        ],
    }


def format_schedule_manifest(manifest: dict[str, list[dict[str, str]]], section: str | None = None) -> str:
    """Render a schedule manifest as plain text for operators."""
    selected_sections = (section,) if section else tuple(manifest.keys())
    lines = ["QMIS scheduled jobs"]
    for section_name in selected_sections:
        for entry in manifest.get(section_name, []):
            lines.append(
                f"[{section_name}] cadence={entry['cadence']} group={entry['group']} command={entry['command']}"
            )
    return "\n".join(lines)
