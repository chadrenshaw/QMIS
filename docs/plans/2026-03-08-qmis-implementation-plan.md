# QMIS Implementation Plan

**Goal:** Rebuild this repository into the Quantitative Macro Intelligence System (QMIS) defined by the engineering spec and addendum, using DuckDB-backed ingestion, feature generation, signal analysis, alerting, and a Rich CLI surface.

**Architecture:** Follow the spec-defined package layout under `src/qmis/`, with collectors writing raw signals into DuckDB, a feature engine materializing derived series, signal engines writing regimes and relationships, and alert/dashboard entrypoints reading those derived tables. The current single-file macro script is a prototype only and should be absorbed or retired in favor of the spec package architecture rather than extended further.

**Tech Stack:** Python 3.12, uv, DuckDB, pandas or polars, scipy, requests, yfinance, fredapi, rich, FastAPI, optional astronomy libraries (`skyfield`, `astral`, `ephem`)

**Execution Mode:** PLAN ONLY - no implementation in this phase

---

## Project: Quantitative Macro Intelligence System

**Goal**: Deliver a local macro intelligence platform aligned to the specs, capable of collecting multi-domain signals, materializing features, detecting macro regimes and relationships, and producing interpretable alerts.

**Timeline**: 6 phases, sequenced by storage and data dependencies

**Team**: 1 engineer

**Constraints**:
- Must follow [QUANT_MACRO_INT_SPEC.md](/Users/crenshaw/Projects/QMIS/docs/specs/QUANT_MACRO_INT_SPEC.md)
- Must follow [QUANT_MACRO_INT_SPEC_ADDENDUM.md](/Users/crenshaw/Projects/QMIS/docs/specs/QUANT_MACRO_INT_SPEC_ADDENDUM.md)
- Must target this repository, which is currently minimal and not yet aligned to the target package structure
- Must use DuckDB as the authoritative local store

---

## Milestones

| # | Milestone | Success Criteria |
|---|-----------|------------------|
| 1 | Foundation Ready | `src/qmis/` package exists, DuckDB bootstraps, run scripts resolve |
| 2 | Core Ingestion Ready | market, macro, and liquidity collectors persist normalized raw signals |
| 3 | Extended Domains Ready | crypto, solar, astronomy, breadth, and optional natural collectors are scaffolded and ingesting supported sources |
| 4 | Feature Layer Ready | feature table materializes 30d/90d/365d metrics and trend labels |
| 5 | Signal Layer Ready | regimes and relationships are computed with guardrails and persisted |
| 6 | Delivery Layer Ready | alerts, CLI dashboard, and scheduling entrypoints operate against derived data |

---

## Phase 1: Foundation, Packaging, And DuckDB Bootstrap

**Files to create**
- `src/qmis/__init__.py`
- `src/qmis/config.py`
- `src/qmis/logger.py`
- `src/qmis/storage.py`
- `src/qmis/schema.py`
- `src/qmis/collectors/__init__.py`
- `src/qmis/features/__init__.py`
- `src/qmis/signals/__init__.py`
- `src/qmis/alerts/__init__.py`
- `src/qmis/dashboard/__init__.py`
- `scripts/run_collectors.py`
- `scripts/run_analysis.py`
- `scripts/run_alerts.py`
- `tests/test_config.py`
- `tests/test_storage.py`
- `tests/test_schema.py`

**Modules to implement**
- configuration loading and environment resolution
- shared logging
- DuckDB connection management
- schema bootstrap for `signals`, `features`, `relationships`, `regimes`
- package entrypoints for collector, analysis, and alert jobs

**Dependencies required**
- `duckdb`
- `pandas` or `polars`
- `rich`
- `requests`
- `scipy`

**Integration points**
- replace the current prototype-only flow in `scripts/macro_sentiment_engine.py` with package-driven scripts over time
- keep the authoritative database under `db/qmis.duckdb`
- align `pyproject.toml` with the spec-defined runtime and package layout

---

## Phase 2: Core Collectors For Market, Macro, And Liquidity

**Files to create**
- `src/qmis/collectors/market.py`
- `src/qmis/collectors/macro.py`
- `src/qmis/collectors/liquidity.py`
- `tests/collectors/test_market.py`
- `tests/collectors/test_macro.py`
- `tests/collectors/test_liquidity.py`

**Modules to implement**
- yfinance-backed market collector
- FRED-backed macro collector
- liquidity collector and liquidity-score input preparation

**Dependencies required**
- `yfinance`
- `fredapi`
- `requests`

**Integration points**
- ingest spec-defined market assets: Gold, Oil, Copper, S&P500, VIX, Dollar Index
- ingest spec-defined macro inputs: 10Y, 3M, PMI, M2, Fed balance sheet, reverse repo
- persist normalized rows into the `signals` table with categories `market`, `macro`, and `liquidity`
- orchestrate collection through `scripts/run_collectors.py`

---

## Phase 3: Expanded Collectors For Crypto, Solar, Astronomy, Breadth, Natural, And News

**Files to create**
- `src/qmis/collectors/crypto.py`
- `src/qmis/collectors/solar.py`
- `src/qmis/collectors/astronomy.py`
- `src/qmis/collectors/breadth.py`
- `src/qmis/collectors/natural.py`
- `src/qmis/collectors/news.py`
- `tests/collectors/test_crypto.py`
- `tests/collectors/test_solar.py`
- `tests/collectors/test_astronomy.py`
- `tests/collectors/test_breadth.py`
- `tests/collectors/test_natural.py`

**Modules to implement**
- crypto market collector
- solar activity collector
- astronomy/lunar/zodiac collector
- breadth collector
- exploratory natural-signal collector
- news collector interface placeholder

**Dependencies required**
- `yfinance`
- `requests`
- `skyfield`
- `astral`
- `ephem`

**Integration points**
- expand `signals.category` support to `crypto`, `astronomy`, `natural`, `breadth`
- store lunar, solar, zodiac, and event-based astronomical signals
- store solar activity and exploratory natural signals with explicit metadata
- breadth and news require source choices not fully specified in the docs, so these tasks include spike work before final source binding

---

## Phase 4: Feature Engine And Feature Materialization

**Files to create**
- `src/qmis/features/trends.py`
- `src/qmis/features/momentum.py`
- `src/qmis/features/volatility.py`
- `src/qmis/features/normalization.py`
- `tests/features/test_trends.py`
- `tests/features/test_momentum.py`
- `tests/features/test_volatility.py`
- `tests/features/test_normalization.py`

**Modules to implement**
- percent change
- moving averages
- rolling volatility
- trend slope
- z-score
- drawdown
- trend labeling

**Dependencies required**
- `numpy`
- `scipy`
- `pandas` or `polars`

**Integration points**
- read raw `signals` from DuckDB
- write spec-defined feature rows into `features`
- use 30d, 90d, and 365d windows with the spec trend thresholds

---

## Phase 5: Signal Analysis Engines

**Files to create**
- `src/qmis/signals/scoring.py`
- `src/qmis/signals/regime.py`
- `src/qmis/signals/thresholds.py`
- `src/qmis/signals/correlations.py`
- `src/qmis/signals/leadlag.py`
- `src/qmis/signals/anomalies.py`
- `src/qmis/signals/cycles.py`
- `tests/signals/test_scoring.py`
- `tests/signals/test_regime.py`
- `tests/signals/test_thresholds.py`
- `tests/signals/test_correlations.py`
- `tests/signals/test_leadlag.py`
- `tests/signals/test_anomalies.py`
- `tests/signals/test_cycles.py`

**Modules to implement**
- macro scoring engine
- regime classifier
- threshold alert candidate generator
- rolling correlation engine
- lead-lag detector
- anomaly detector for broken relationships
- FFT cycle detector

**Dependencies required**
- `numpy`
- `scipy`

**Integration points**
- compute the four macro scores: inflation, growth, liquidity, risk
- write regime outputs into `regimes`
- write relationship outputs into `relationships`
- support 30d/90d/365d/5y windows
- support lag sweeps from `-365` to `+365`
- apply multiple-testing correction, persistence thresholds, and confidence labeling per addendum

---

## Phase 6: Alerting, CLI Dashboard, Scheduling, And Optional API

**Files to create**
- `src/qmis/alerts/engine.py`
- `src/qmis/alerts/rules.py`
- `src/qmis/dashboard/cli.py`
- `src/qmis/api.py`
- `tests/alerts/test_engine.py`
- `tests/alerts/test_rules.py`
- `tests/dashboard/test_cli.py`
- `tests/test_api.py`

**Modules to implement**
- alert routing and rule evaluation
- Rich CLI dashboard
- scheduling-ready command surfaces
- optional FastAPI read API

**Dependencies required**
- `rich`
- `fastapi`
- `uvicorn`

**Integration points**
- alert engine consumes `regimes`, `relationships`, and threshold outputs
- CLI surfaces macro dashboard plus correlation and anomaly summaries
- `scripts/run_collectors.py`, `scripts/run_analysis.py`, and `scripts/run_alerts.py` become the cron/systemd hooks
- optional API exposes read-only outputs for Grafana or future UI work

---

## Dependencies Map

```text
Phase 1 ──> Phase 2 ──> Phase 4 ──> Phase 5 ──> Phase 6
   └──────────────> Phase 3 ───────┘
```

Critical path:
- storage and schema must exist before any collector writes
- collectors must populate raw signals before features can materialize
- features must exist before correlation/regime engines can produce stable outputs
- alerts and dashboard depend on derived data from the signal engines

---

## Risks And Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Spec/source mismatch for breadth and news | Medium | High | Treat as spike tasks before source binding |
| Overextending the prototype script | High | Medium | Freeze prototype and migrate to `src/qmis/` package architecture |
| Cross-domain false positives | High | High | Implement statistical guardrails from the addendum before broad alerting |
| Data source instability | High | Medium | Keep collectors isolated and metadata-rich for retry/debugging |

---

## Documentation Confirmation

Statement:
Documentation updates are already part of this planning task via the saved implementation plan. Additional documentation changes should be scheduled alongside implementation phases once code begins.
