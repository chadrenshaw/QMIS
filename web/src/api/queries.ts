import { QueryClient, useQuery } from "@tanstack/react-query";
import { fetchJson } from "./client";

export type HealthResponse = {
  status: string;
  db_path: string;
  read_only: boolean;
};

export type DashboardTrend = {
  ts: string;
  trend_label: string;
};

export type DashboardSignal = {
  ts: string;
  value: number;
  unit: string;
  category?: string;
  source?: string;
};

export type DashboardSignalPoint = {
  ts: string;
  value: number;
  unit: string;
};

export type DashboardScorePoint = {
  ts: string;
  inflation_score: number;
  growth_score: number;
  liquidity_score: number;
  risk_score: number;
  regime_label: string;
  confidence: number;
};

export type DashboardRelationship = {
  ts: string;
  series_x: string;
  series_y: string;
  window_days: number;
  lag_days: number;
  correlation: number;
  p_value: number;
  relationship_state: string;
  confidence_label: string;
};

export type DashboardAnomaly = {
  ts: string;
  series_x: string;
  series_y: string;
  anomaly_type: string;
  historical_state: string;
  current_state: string;
  historical_window_days: number;
  current_window_days: number;
  historical_correlation: number;
  current_correlation: number;
};

export type DashboardFreshness = {
  status: string;
  latest_signal_ts?: string | null;
  latest_regime_ts?: string | null;
  latest_relationship_ts?: string | null;
  age_days?: number;
};

export type DashboardAlertSummary = {
  status: string;
  message: string;
  updated_at: string | null;
  alerts: Array<Record<string, unknown>>;
};

export type DashboardSnapshot = {
  trend_summary: Record<string, DashboardTrend>;
  signal_summary: Record<string, DashboardSignal>;
  signal_groups: Record<string, string[]>;
  signal_history: Record<string, DashboardSignalPoint[]>;
  scores: Record<string, number>;
  score_history: DashboardScorePoint[];
  regime: {
    ts: string;
    inflation_score: number;
    growth_score: number;
    liquidity_score: number;
    risk_score: number;
    regime_label: string;
    confidence: number;
  } | null;
  yield_curve: number | null;
  yield_curve_state: string;
  freshness: DashboardFreshness;
  latest_snapshot_ts: string | null;
  top_relationships: DashboardRelationship[];
  lead_lag_relationships: DashboardRelationship[];
  anomalies: DashboardAnomaly[];
  alert_summary: DashboardAlertSummary;
};

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30_000,
    },
  },
});

export function useHealthQuery() {
  return useQuery({
    queryKey: ["health"],
    queryFn: () => fetchJson<HealthResponse>("/health"),
  });
}

export function useDashboardQuery() {
  return useQuery({
    queryKey: ["dashboard"],
    queryFn: () => fetchJson<DashboardSnapshot>("/dashboard"),
  });
}
