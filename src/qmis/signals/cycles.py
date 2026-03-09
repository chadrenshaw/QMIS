"""FFT-based cycle detection for QMIS series."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


KNOWN_CYCLES = {
    "lunar_period": 29.53,
    "solar_cycle": 11.0 * 365.25,
}


def _median_step_days(ts: pd.Series) -> float:
    diffs = ts.sort_values().diff().dropna()
    if diffs.empty:
        return 1.0
    return float(diffs.dt.total_seconds().median() / 86400.0) or 1.0


def _classify_cycle_confidence(relative_power: float, matched_cycle: str | None) -> str:
    if matched_cycle and relative_power >= 0.45:
        return "validated"
    if relative_power >= 0.2:
        return "statistically_significant"
    if relative_power >= 0.1:
        return "tentative"
    return "likely_spurious"


def detect_dominant_cycles(signal_frame: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Detect the dominant FFT periods for each series in a signal frame."""
    columns = [
        "series_name",
        "period_days",
        "frequency",
        "power",
        "relative_power",
        "matched_cycle",
        "confidence_label",
    ]
    if signal_frame.empty:
        return pd.DataFrame(columns=columns)

    frame = signal_frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    rows: list[dict[str, object]] = []

    for series_name, series in frame.groupby("series_name", sort=True):
        series = series.sort_values("ts").reset_index(drop=True)
        values = series["value"].to_numpy(dtype=float)
        if len(values) < 32:
            continue

        centered = values - np.mean(values)
        sample_spacing = _median_step_days(series["ts"])
        spectrum = np.fft.rfft(centered)
        frequencies = np.fft.rfftfreq(len(centered), d=sample_spacing)
        powers = np.abs(spectrum) ** 2

        positive = frequencies > 0
        frequencies = frequencies[positive]
        powers = powers[positive]
        if len(frequencies) == 0:
            continue

        total_power = float(np.sum(powers))
        top_indices = np.argsort(powers)[::-1][:top_n]
        for index in top_indices:
            frequency = float(frequencies[index])
            if frequency <= 0:
                continue
            period_days = 1.0 / frequency
            power = float(powers[index])
            relative_power = power / total_power if total_power > 0 else 0.0

            matched_cycle = None
            for cycle_name, cycle_period in KNOWN_CYCLES.items():
                tolerance = max(1.5, cycle_period * 0.05)
                if math.isclose(period_days, cycle_period, abs_tol=tolerance):
                    matched_cycle = cycle_name
                    break

            rows.append(
                {
                    "series_name": str(series_name),
                    "period_days": float(period_days),
                    "frequency": frequency,
                    "power": power,
                    "relative_power": relative_power,
                    "matched_cycle": matched_cycle,
                    "confidence_label": _classify_cycle_confidence(relative_power, matched_cycle),
                }
            )

    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    return result.sort_values(["series_name", "power"], ascending=[True, False]).reset_index(drop=True)
