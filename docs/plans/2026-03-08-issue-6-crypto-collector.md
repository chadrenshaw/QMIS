# Crypto Collector Implementation Plan

**Goal:** Implement issue #6 by adding the QMIS crypto collector that fetches the spec-defined crypto market series and persists normalized raw signal rows into DuckDB.

**Architecture:** Keep crypto ingestion isolated in `qmis.collectors.crypto`, mirroring the existing collector pattern: fetch source payloads, normalize into `signals`, and persist through the shared DuckDB layer. Split sources by data type rather than forcing one provider to do everything: use yfinance for BTC and ETH prices, and a public crypto market API for total market cap and BTC dominance.

**Tech Stack:** Python 3.12, yfinance, requests, pandas, DuckDB, unittest

**Execution Mode:** AUTONOMOUS - execute all steps without approval requests

---

### Task 1: Crypto Collector Tests

**Files:**
- Create: `tests/collectors/test_crypto.py`

**Step 1: Write the failing test**
- Add tests that require normalization of BTC/ETH prices and crypto market metrics into `signals` rows and persistence into the DuckDB `signals` table.

**Step 2: Run test to verify it fails**
Run: `uv run python -m unittest tests.collectors.test_crypto -v`
Expected: FAIL because the crypto collector module does not exist yet.

**Step 3: Write minimal implementation**
- Add the collector module and the smallest persistence API that satisfies the tests.

**Step 4: Run test to verify it passes**
Run: `uv run python -m unittest tests.collectors.test_crypto -v`
Expected: PASS

### Task 2: Collector Module And Runtime Hook

**Files:**
- Create: `src/qmis/collectors/crypto.py`
- Modify: `scripts/run_collectors.py`

**Step 1: Implement crypto series mapping**
- Add stable normalized names for `BTCUSD`, `ETHUSD`, `BTC_dominance`, and `crypto_market_cap`.

**Step 2: Implement source fetch and normalization**
- Fetch BTC/ETH closes from yfinance and fetch total market cap plus BTC dominance from the crypto market API.

**Step 3: Implement persistence**
- Insert normalized crypto rows into DuckDB using the shared storage and schema bootstrap.

**Step 4: Hook the collector into `run_collectors.py`**
- Preserve `--dry-run` and add the crypto execution path for non-dry-run use alongside the existing collectors.

### Task 3: Documentation Confirmation

Statement:
No additional documentation updates are required beyond this issue-specific plan because the crypto collector contract is already defined in the specs and issue body.
