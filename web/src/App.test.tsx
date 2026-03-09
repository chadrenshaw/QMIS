import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import App from "./App";

vi.mock("./api/queries", () => ({
  useHealthQuery: () => ({
    isSuccess: true,
    isLoading: false,
  }),
  useDashboardQuery: () => ({
    isLoading: false,
    isError: false,
    data: {
      trend_summary: {
        gold: { ts: "2026-03-08T00:00:00", trend_label: "UP" },
        oil: { ts: "2026-03-08T00:00:00", trend_label: "UP" },
        BTCUSD: { ts: "2026-03-08T00:00:00", trend_label: "UP" },
      },
      signal_summary: {
        gold: { ts: "2026-03-08T00:00:00", value: 2150, unit: "usd", category: "market" },
        oil: { ts: "2026-03-08T00:00:00", value: 84.5, unit: "usd", category: "market" },
        BTCUSD: { ts: "2026-03-08T00:00:00", value: 95000, unit: "usd", category: "crypto" },
        yield_10y: { ts: "2026-03-08T00:00:00", value: 4.2, unit: "percent", category: "macro" },
        yield_3m: { ts: "2026-03-08T00:00:00", value: 3.8, unit: "percent", category: "macro" },
        fed_balance_sheet: {
          ts: "2026-03-08T00:00:00",
          value: 7100,
          unit: "billions_usd",
          category: "liquidity",
        },
      },
      scores: {
        inflation_score: 3,
        growth_score: 1,
        liquidity_score: 2,
        risk_score: 2,
      },
      regime: {
        ts: "2026-03-08T00:00:00",
        inflation_score: 3,
        growth_score: 1,
        liquidity_score: 2,
        risk_score: 2,
        regime_label: "STAGFLATION RISK",
        confidence: 0.82,
      },
      yield_curve: 0.4,
      yield_curve_state: "NORMAL",
      latest_snapshot_ts: "2026-03-08T00:00:00",
      freshness: {
        status: "fresh",
        latest_signal_ts: "2026-03-08T00:00:00",
        latest_regime_ts: "2026-03-08T00:00:00",
        latest_relationship_ts: "2026-03-08T00:00:00",
      },
      signal_groups: {
        market: ["gold", "oil"],
        macro: ["yield_10y", "yield_3m"],
        liquidity: ["fed_balance_sheet"],
        crypto: ["BTCUSD"],
      },
      signal_history: {
        gold: [
          { ts: "2026-03-07T00:00:00", value: 2100, unit: "usd" },
          { ts: "2026-03-08T00:00:00", value: 2150, unit: "usd" },
        ],
        oil: [
          { ts: "2026-03-07T00:00:00", value: 82, unit: "usd" },
          { ts: "2026-03-08T00:00:00", value: 84.5, unit: "usd" },
        ],
        BTCUSD: [
          { ts: "2026-03-07T00:00:00", value: 93500, unit: "usd" },
          { ts: "2026-03-08T00:00:00", value: 95000, unit: "usd" },
        ],
      },
      score_history: [
        {
          ts: "2026-03-07T00:00:00",
          inflation_score: 2,
          growth_score: 2,
          liquidity_score: 2,
          risk_score: 1,
          regime_label: "INFLATIONARY EXPANSION",
        },
        {
          ts: "2026-03-08T00:00:00",
          inflation_score: 3,
          growth_score: 1,
          liquidity_score: 2,
          risk_score: 2,
          regime_label: "STAGFLATION RISK",
        },
      ],
      alert_summary: {
        status: "watch",
        message: "3 active alert(s).",
        updated_at: "2026-03-08T00:00:00",
        alerts: [
          {
            ts: "2026-03-08T00:00:00",
            alert_type: "regime_change",
            severity: "info",
            title: "Regime change detected",
            message: "Macro regime moved from INFLATIONARY EXPANSION to STAGFLATION RISK.",
          },
        ],
      },
      top_relationships: [
        {
          ts: "2026-03-08T00:00:00",
          series_x: "gold",
          series_y: "yield_10y",
          window_days: 365,
          lag_days: 0,
          correlation: -0.82,
          p_value: 0.0001,
          relationship_state: "stable",
          confidence_label: "validated",
        },
      ],
      lead_lag_relationships: [
        {
          ts: "2026-03-08T00:00:00",
          series_x: "sunspot_number",
          series_y: "BTCUSD",
          window_days: 365,
          lag_days: 28,
          correlation: 0.63,
          p_value: 0.01,
          relationship_state: "exploratory",
          confidence_label: "exploratory",
        },
      ],
      anomalies: [
        {
          ts: "2026-03-08T00:00:00",
          series_x: "gold",
          series_y: "yield_10y",
          anomaly_type: "relationship_break",
          historical_state: "stable",
          current_state: "broken",
          historical_window_days: 365,
          current_window_days: 30,
          historical_correlation: -0.82,
          current_correlation: -0.05,
        },
      ],
    },
  }),
  queryClient: {},
}));

describe("App", () => {
  it("renders the operator console shell with regime content", () => {
    render(<App />);

    expect(screen.getByText("Quant Macro Intelligence System")).toBeInTheDocument();
    expect(screen.getAllByText("STAGFLATION RISK")).toHaveLength(2);
    expect(screen.getByText("Current score stack")).toBeInTheDocument();
    expect(screen.getByText("Signal history")).toBeInTheDocument();
    expect(screen.getAllByText("Score history")).toHaveLength(2);
    expect(screen.getByText("Market Signals")).toBeInTheDocument();
    expect(screen.getByText("Relationship summary")).toBeInTheDocument();
    expect(screen.getByText("relationship_break")).toBeInTheDocument();
    expect(screen.getByText("3 active alert(s).")).toBeInTheDocument();
  });
});
