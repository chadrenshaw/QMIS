# Quantitative Macro Intelligence System (QMIS)
## Engineering Specification

Version: v2  
Language: Python 3.12  
Environment: uv + pyproject.toml  
Storage: DuckDB  
Primary Runtime: CLI + scheduled jobs  
Optional UI: Grafana

---

# 1. System Purpose

The Quantitative Macro Intelligence System (QMIS) collects global signals from financial markets, macroeconomic indicators, crypto markets, and external environmental data sources.

The system analyzes these signals to:

- Detect macroeconomic regimes
- Identify correlations across domains
- Detect leading indicators
- Identify anomalies and broken relationships
- Generate alerts about potential macro shifts

The system should function as a **local macro intelligence platform** capable of discovering relationships and generating interpretable signals.

---

# 2. High-Level Architecture

```
DATA SOURCES
      │
      ▼
COLLECTORS
      │
      ▼
RAW DATA STORE
    DuckDB
      │
      ▼
FEATURE ENGINE
      │
      ▼
SIGNAL ANALYSIS ENGINE
(regime + correlations)
      │
      ▼
ALERT ENGINE
      │
      ▼
CLI / API / DASHBOARD
```

---

# 3. Technology Stack

Component | Technology
---|---
Runtime | Python 3.12
Environment | uv
Data Storage | DuckDB
DataFrames | Polars or Pandas
Visualization | Rich (CLI) / Grafana
API | FastAPI
Scheduling | cron / systemd timers
Package Manager | uv

---

# 4. Project Structure

```
macro-intelligence/

pyproject.toml
uv.lock
README.md

src/qmis/

    config.py
    logger.py
    storage.py
    schema.py

    collectors/
        market.py
        macro.py
        crypto.py
        solar.py
        news.py

    features/
        trends.py
        momentum.py
        volatility.py
        normalization.py

    signals/
        scoring.py
        regime.py
        thresholds.py
        correlations.py
        leadlag.py
        anomalies.py

    alerts/
        engine.py
        rules.py

    dashboard/
        cli.py

scripts/

    run_collectors.py
    run_analysis.py
    run_alerts.py
```

---

# 5. Data Storage Schema

Database: DuckDB

## Raw Signals Table

```
signals
ts TIMESTAMP
source TEXT
category TEXT
series_name TEXT
value DOUBLE
unit TEXT
metadata JSON
```

Example rows:

| ts | category | series_name | value |
|---|---|---|---|
2025-02-01 | market | BTCUSD | 62000
2025-02-01 | macro | yield_10y | 4.21
2025-02-01 | solar | sunspot_number | 124

---

## Feature Table

Derived time-series features.

```
features
ts
series_name
pct_change_30d
pct_change_90d
pct_change_365d
zscore_30d
volatility_30d
slope_30d
drawdown_90d
trend_label
```

---

## Relationship Table

```
relationships
ts
series_x
series_y
window_days
lag_days
correlation
p_value
relationship_state
```

relationship_state values:

- stable
- emerging
- weakening
- broken

---

## Regime Table

```
regimes
ts
inflation_score
growth_score
liquidity_score
risk_score
regime_label
confidence
```

---

# 6. Data Collectors

Collectors ingest raw data and run independently.

## Market Collector

Source: yfinance

Assets:

- Gold (GC=F)
- Oil (CL=F)
- Copper (HG=F)
- S&P500 (^GSPC)
- VIX (^VIX)
- Dollar Index (DX-Y.NYB)

---

## Macro Collector

Source: FRED

Indicators:

- 10Y Treasury Yield
- 3M Treasury Yield
- M2 Money Supply
- Fed Balance Sheet
- Reverse Repo
- PMI

---

## Crypto Collector

Source: yfinance / public APIs

Indicators:

- BTC price
- ETH price
- BTC dominance
- crypto market cap

---

## Solar Collector

Source: NOAA / SIDC

Indicators:

- sunspot number
- solar flux
- geomagnetic index
- solar flare count

---

# 7. Feature Engine

Feature engine generates derived metrics.

Features computed:

- percent_change
- moving_average
- rolling_volatility
- trend_slope
- z_score
- drawdown

Trend classification rules:

```
change > +5% → UP
change < -5% → DOWN
else → SIDEWAYS
```

Windows used:

- 30 days
- 90 days
- 365 days

---

# 8. Correlation Engine

Compute rolling correlations.

```
corr(X, Y)
```

Windows:

- 30 days
- 90 days
- 365 days

Alert condition:

```
abs(correlation) > 0.6
p_value < 0.05
```

---

# 9. Lead-Lag Detection

Test whether one signal leads another.

Method: cross-correlation.

```
for lag in range(-90, 90):
    compute correlation
```

Example result:

```
Sunspots lead Bitcoin by 28 days
Correlation: 0.54
```

---

# 10. Macro Regime Engine

Calculate four macro scores.

## Inflation Score

Inputs:

- oil
- gold
- breakeven inflation

---

## Growth Score

Inputs:

- copper
- S&P500
- PMI

---

## Liquidity Score

Inputs:

- Fed balance sheet
- M2
- reverse repo
- dollar index

---

## Risk Score

Inputs:

- VIX
- credit spreads
- yield curve

---

## Regime Classification

Possible regimes:

- Inflationary Expansion
- Disinflation
- Recession Risk
- Liquidity Expansion
- Liquidity Withdrawal
- Crisis / Risk-Off
- Speculative Bubble
- Neutral

---

# 11. Alert Engine

Alert categories.

## Regime Change

Example:

```
REGIME CHANGE
Neutral → Stagflation Risk
```

---

## Threshold Alerts

Examples:

```
VIX > 25
Yield curve inverted
BTC down 15% in 30 days
```

---

## Correlation Alerts

Example:

```
SIGNIFICANT CORRELATION DETECTED
Sunspots vs Bitcoin
90-day correlation: 0.64
```

---

## Relationship Break Alerts

Example:

```
ANOMALY
Gold historically inverse to real yields
Relationship broken
```

---

# 12. CLI Dashboard

Example CLI output:

```
GLOBAL MACRO DASHBOARD

Gold Trend: UP
Oil Trend: UP
Copper Trend: DOWN

BTC Trend: UP

Yield Curve: INVERTED
VIX: 19

Macro Scores

Inflation: 3
Growth: 1
Liquidity: 2
Risk: 2

Current Regime

STAGFLATION RISK
```

Use the **Rich** Python library for formatting.

---

# 13. Scheduling

Recommended job schedule.

Market collectors:

```
every 15 minutes
```

Macro collectors:

```
daily
```

Solar collectors:

```
daily
```

Correlation engine:

```
daily
```

Regime engine:

```
daily
```

---

# 14. Logging

Log system events including:

- data ingestion
- feature generation
- regime changes
- alerts

Logs should be written to structured local files.

---

# 15. Phase Implementation Plan

## Phase 1

Core system

- collectors
- DuckDB storage
- trend engine
- CLI dashboard

---

## Phase 2

Signal discovery

- correlation engine
- lead-lag detection
- alert engine

---

## Phase 3

External signals

- solar indicators
- expanded macro signals
- crypto analytics

---

## Phase 4

Advanced forecasting

- anomaly detection
- regime probability model
- machine learning forecasts

---

# 16. Future Expansion

Potential system upgrades:

- graph relationship visualization
- Grafana dashboards
- automated macro reports
- AI-generated macro narratives
- probabilistic forecasting

---

# 17. System Goal

The system should evolve into a **macro intelligence engine** capable of:

- detecting macro regime shifts
- discovering signal relationships
- identifying early warning indicators
- monitoring global financial sentiment across domains
