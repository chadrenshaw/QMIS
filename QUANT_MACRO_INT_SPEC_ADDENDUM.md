# QMIS Specification Addendum
## Additional Signals, Correlation Engine Expansion, and Astronomical Modules

This document extends the **Quantitative Macro Intelligence System (QMIS) v2 specification** with additional data domains, analysis engines, and signal categories discussed after the original spec.

The additions focus on expanding the system from a macro market monitor into a **multi-domain signal discovery platform**.

---

# 1. New Data Domains

The system will support the following additional signal categories:

| Category | Description |
|---|---|
crypto | digital asset markets |
astronomy | lunar, solar, and celestial cycles |
natural | earth and space environmental signals |
liquidity | monetary and capital flow indicators |
breadth | internal market participation metrics |

These categories extend the original signal schema.

---

# 2. Updated Signal Categories

The `signals.category` field now supports:

```
market
macro
crypto
astronomy
natural
liquidity
breadth
```

Example rows:

| ts | category | series_name | value |
|---|---|---|---|
2026-03-08 | crypto | BTCUSD | 68400 |
2026-03-08 | astronomy | lunar_cycle_day | 18.4 |
2026-03-08 | astronomy | sunspot_number | 121 |
2026-03-08 | natural | geomagnetic_kp | 3.2 |

---

# 3. Crypto Market Module

Add a new collector:

```
collectors/crypto.py
```

## Crypto Signals

| Signal | Source |
|---|---|
BTC price | yfinance |
ETH price | yfinance |
BTC dominance | crypto APIs |
crypto market cap | crypto APIs |
stablecoin supply | optional |
crypto volume | optional |

Example series names:

```
BTCUSD
ETHUSD
BTC_dominance
crypto_market_cap
stablecoin_supply
```

---

# 4. Astronomical Signals Module

Add collector:

```
collectors/astronomy.py
```

Recommended libraries:

```
skyfield
astral
ephem
```

## Lunar Cycle Signals

Daily computed signals:

| Series | Description |
|---|---|
lunar_cycle_day | position in 29.53 day cycle |
lunar_phase_angle | moon phase angle |
lunar_illumination | % illumination |
moon_distance | earth-moon distance |
moon_declination | declination angle |

Example row:

```
ts: 2026-03-08
series_name: lunar_cycle_day
value: 18.2
```

---

## Zodiac Position Signals

Store solar ecliptic longitude:

```
solar_longitude
```

Range:

```
0°–360°
```

Derived zodiac index:

```
zodiac_index = floor(solar_longitude / 30)
```

Optional label:

```
zodiac_sign
```

Mapping:

| Sign | Range |
|---|---|
Aries | 0-30° |
Taurus | 30-60° |
Gemini | 60-90° |
... | ... |
Pisces | 330-360° |

---

## Axial Precession Signals

For completeness store:

```
earth_axial_tilt
precession_angle
```

These change extremely slowly but allow long-term historical comparisons.

---

# 5. Solar Activity Signals

Add real solar activity data from NOAA or SIDC.

Collector:

```
collectors/solar.py
```

Signals:

| Series | Description |
|---|---|
sunspot_number | daily sunspot count |
solar_flux_f107 | radio flux |
geomagnetic_kp | geomagnetic disturbance |
solar_flare_count | daily flare count |

Example:

```
series_name: sunspot_number
value: 143
```

---

# 6. Liquidity Indicators

Add additional macro-financial liquidity signals.

Collector:

```
collectors/liquidity.py
```

Signals:

| Series | Source |
|---|---|
fed_balance_sheet | FRED |
m2_money_supply | FRED |
reverse_repo_usage | FRED |
dollar_index | yfinance |

Derived indicator:

```
global_liquidity_score
```

---

# 7. Market Breadth Signals

Collector:

```
collectors/breadth.py
```

Signals:

| Series | Description |
|---|---|
sp500_above_200dma | % of stocks above 200 day MA |
advance_decline_line | market breadth |
new_highs | count |
new_lows | count |

Breadth deterioration often precedes market crashes.

---

# 8. Correlation Engine Expansion

Original correlation engine now supports **cross-domain analysis**.

Example relationships:

```
sunspots vs BTC
lunar_phase vs VIX
solar_flux vs oil
geomagnetic_kp vs equities
crypto_market_cap vs liquidity_score
```

---

## Correlation Windows

Add longer analysis windows:

```
30 days
90 days
365 days
5 years
```

Alert rule:

```
abs(correlation) > 0.6
p_value < 0.05
```

---

# 9. Lead-Lag Detection Expansion

Add extended lag detection.

Range:

```
-365 to +365 days
```

Example result:

```
Sunspots lead BTC by 28 days
Correlation: 0.54
```

Store results in `relationships` table.

---

# 10. Event-Based Signals

Add binary event signals.

Examples:

```
full_moon
new_moon
major_solar_flare
geomagnetic_storm
```

Event signals allow testing price behavior around events.

Example analysis:

```
BTC returns around full moon
SP500 returns after solar flares
```

---

# 11. Cyclic Analysis Engine

Add spectral analysis module.

Location:

```
signals/cycles.py
```

Method:

```
FFT (Fast Fourier Transform)
```

Purpose:

Detect periodicities in financial signals.

Compare detected frequencies with known cycles:

```
lunar_period ≈ 29.53 days
solar_cycle ≈ 11 years
```

---

# 12. Relationship Stability Tracking

Relationships table gains new states:

```
stable
emerging
weakening
broken
exploratory
```

Engine logic tracks how relationships evolve over time.

Example anomaly:

```
Gold historically inverse to real yields
Correlation collapsed
```

---

# 13. Additional Natural Signals (Experimental)

Add optional natural phenomena collectors.

Collector:

```
collectors/natural.py
```

Signals:

| Series | Source |
|---|---|
earthquake_count | USGS |
global_temperature_anomaly | NOAA |
geomagnetic_activity | NOAA |
solar_wind_speed | NASA |

These signals are considered **exploratory**.

---

# 14. Statistical Guardrails

Because many signals will be tested simultaneously, implement safeguards.

Required methods:

```
multiple testing correction
minimum persistence threshold
out-of-sample validation
```

Possible approaches:

```
Bonferroni correction
False discovery rate (FDR)
```

---

# 15. Correlation Confidence Levels

All discovered correlations must be labeled:

```
validated
statistically_significant
tentative
exploratory
likely_spurious
```

Labels determined using:

- correlation magnitude
- statistical significance
- persistence across windows
- historical stability

---

# 16. New Alert Types

Add two additional alert classes.

## Correlation Discovery

Example:

```
CORRELATION DETECTED
Sunspots vs Bitcoin

Window: 365 days
Correlation: 0.63
Lag: 28 days
Confidence: exploratory
```

## Cyclic Signal Match

Example:

```
PERIODICITY DETECTED

BTC dominant cycle: 29.4 days
Matches lunar cycle period
Confidence: weak
```

---

# 17. Extended System Goal

After these additions the system evolves from a macro dashboard into a **multi-domain signal discovery engine** capable of analyzing relationships across:

```
financial markets
macroeconomic indicators
crypto markets
astronomical cycles
solar activity
natural phenomena
```

The system aims to detect:

```
macro regime shifts
leading indicators
cross-domain correlations
anomalies in historical relationships
emerging market cycles
```

These capabilities enable automated discovery of signals that may precede major economic or market events.
