import { PanelFrame } from "./components/PanelFrame";
import { RelationshipTable } from "./components/RelationshipTable";
import { ScoreBars } from "./components/ScoreBars";
import { SignalRow } from "./components/SignalRow";
import { useDashboardQuery, useHealthQuery, type DashboardSnapshot } from "./api/queries";
import { AlertSummaryPanel } from "./components/AlertSummaryPanel";
import { HistoryChart } from "./components/HistoryChart";
import { SectionHeading } from "./components/SectionHeading";
import { StatusBadge } from "./components/StatusBadge";

const MAJOR_SIGNALS = [
  { series: "gold", label: "Gold Trend" },
  { series: "oil", label: "Oil Trend" },
  { series: "copper", label: "Copper Trend" },
  { series: "BTCUSD", label: "BTC Trend" },
] as const;

const SIGNAL_GROUP_LABELS: Record<string, string> = {
  market: "Market Signals",
  macro: "Macro Signals",
  liquidity: "Liquidity Signals",
  crypto: "Crypto Signals",
  astronomy: "Astronomy Signals",
  natural: "Natural Signals",
};

const SIGNAL_HISTORY_SERIES = [
  { key: "gold", label: "Gold", color: "#b7791f" },
  { key: "oil", label: "Oil", color: "#b45309" },
  { key: "BTCUSD", label: "BTC", color: "#1d4ed8" },
] as const;

const SCORE_HISTORY_SERIES = [
  { key: "inflation_score", label: "Inflation", color: "#cf5f18" },
  { key: "growth_score", label: "Growth", color: "#227c9d" },
  { key: "liquidity_score", label: "Liquidity", color: "#2f855a" },
  { key: "risk_score", label: "Risk", color: "#9f1239" },
] as const;

function formatTimestamp(value?: string | null) {
  if (!value) {
    return "No snapshot timestamp";
  }
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

function buildSignalHistory(snapshot: DashboardSnapshot) {
  const timestamps = new Set<string>();
  for (const series of SIGNAL_HISTORY_SERIES) {
    for (const point of snapshot.signal_history[series.key] ?? []) {
      timestamps.add(point.ts);
    }
  }

  return Array.from(timestamps)
    .sort()
    .map((ts) => {
      const row: Record<string, string | number | null> = { ts };
      for (const series of SIGNAL_HISTORY_SERIES) {
        const point = (snapshot.signal_history[series.key] ?? []).find((candidate) => candidate.ts === ts);
        row[series.key] = point?.value ?? null;
      }
      return row;
    });
}

function buildScoreHistory(snapshot: DashboardSnapshot) {
  return snapshot.score_history.map((point) => ({
    ts: point.ts,
    inflation_score: point.inflation_score,
    growth_score: point.growth_score,
    liquidity_score: point.liquidity_score,
    risk_score: point.risk_score,
  }));
}

function freshnessTone(status?: string) {
  if (status === "fresh") {
    return "fresh";
  }
  if (status === "stale") {
    return "stale";
  }
  if (status === "empty") {
    return "unavailable";
  }
  return "neutral";
}

function HealthStatus({
  apiOnline,
  freshnessLabel,
  regimeLabel,
  latestSnapshotTs,
}: {
  apiOnline: boolean;
  freshnessLabel?: string;
  regimeLabel?: string | null;
  latestSnapshotTs?: string | null;
}) {
  return (
    <div className="flex flex-col gap-3 rounded-[1.5rem] border border-slate-200/70 bg-white/75 p-4 backdrop-blur">
      <span className="font-mono text-xs uppercase tracking-[0.28em] text-slate-500">System Status</span>
      <div className="flex items-center gap-3">
        <span className={`h-3 w-3 rounded-full ${apiOnline ? "bg-emerald-500" : "bg-rose-500"}`} />
        <span className="font-display text-lg text-slate-900">{apiOnline ? "API ONLINE" : "API DEGRADED"}</span>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge label={freshnessLabel ?? "unknown"} tone={freshnessTone(freshnessLabel)} />
        <span className="font-mono text-[0.68rem] uppercase tracking-[0.18em] text-slate-500">
          Snapshot {formatTimestamp(latestSnapshotTs)}
        </span>
      </div>
      <div className="font-mono text-xs uppercase tracking-[0.22em] text-slate-500">
        {regimeLabel ?? "Waiting for regime snapshot"}
      </div>
    </div>
  );
}

function SignalGroups({ snapshot }: { snapshot: DashboardSnapshot }) {
  const categories = Object.entries(snapshot.signal_groups);

  if (categories.length === 0) {
    return <p className="text-sm text-slate-500">No grouped signal data available yet.</p>;
  }

  return (
    <div className="grid gap-5 xl:grid-cols-2">
      {categories.map(([category, seriesNames]) => (
        <div key={category} className="rounded-[1.5rem] border border-slate-200/80 bg-white/80 p-5">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h3 className="font-display text-lg text-slate-950">{SIGNAL_GROUP_LABELS[category] ?? category}</h3>
            <StatusBadge label={category} tone="neutral" />
          </div>
          <div className="grid gap-3">
            {seriesNames.map((seriesName) => (
              <SignalRow
                key={seriesName}
                label={seriesName}
                trend={snapshot.trend_summary[seriesName]?.trend_label ?? "N/A"}
                value={snapshot.signal_summary[seriesName]?.value}
                unit={snapshot.signal_summary[seriesName]?.unit}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const healthQuery = useHealthQuery();
  const dashboardQuery = useDashboardQuery();

  const snapshot = dashboardQuery.data;
  const hasData = Boolean(snapshot?.regime || Object.keys(snapshot?.trend_summary ?? {}).length > 0);

  return (
    <main className="min-h-screen bg-canvas text-ink">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <section className="relative overflow-hidden rounded-[2rem] border border-white/60 bg-shell px-6 py-8 shadow-dashboard">
          <div className="absolute inset-0 bg-grid bg-[size:34px_34px] opacity-30" />
          <div className="absolute -right-20 top-0 h-48 w-48 rounded-full bg-accent/20 blur-3xl" />
          <div className="absolute left-10 top-10 h-32 w-32 rounded-full bg-signal/20 blur-3xl" />

          <div className="relative flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-2xl">
              <p className="font-mono text-xs uppercase tracking-[0.38em] text-slate-500">
                Quant Macro Intelligence System
              </p>
              <h1 className="mt-3 font-display text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">
                Operator console for regime, signal, and relationship drift.
              </h1>
              <p className="mt-4 max-w-xl text-sm leading-6 text-slate-600 sm:text-base">
                Browser dashboard for the QMIS read API. The web view mirrors the system model: signals feed features,
                features feed regimes and relationships, and alerts sit on top as the final operator layer.
              </p>
            </div>

            <HealthStatus
              apiOnline={Boolean(healthQuery.isSuccess)}
              freshnessLabel={snapshot?.freshness?.status}
              regimeLabel={snapshot?.regime?.regime_label}
              latestSnapshotTs={snapshot?.latest_snapshot_ts}
            />
          </div>
        </section>

        <div className="mt-6 flex flex-wrap gap-2">
          {["Signals", "Features", "Regimes + Relationships", "Alerts"].map((label) => (
            <StatusBadge key={label} label={label} tone="neutral" />
          ))}
        </div>

        {dashboardQuery.isLoading ? (
          <div className="mt-6 grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
            <PanelFrame eyebrow="Loading" title="Fetching derived snapshot">
              <div className="space-y-3">
                <div className="h-16 animate-pulse rounded-2xl bg-slate-200/70" />
                <div className="h-16 animate-pulse rounded-2xl bg-slate-200/60" />
                <div className="h-40 animate-pulse rounded-3xl bg-slate-200/50" />
              </div>
            </PanelFrame>
            <PanelFrame eyebrow="Loading" title="Staging dashboard layers">
              <div className="h-80 animate-pulse rounded-3xl bg-slate-200/50" />
            </PanelFrame>
          </div>
        ) : dashboardQuery.isError ? (
          <div className="mt-6 rounded-[1.75rem] border border-rose-200 bg-rose-50 p-6 text-rose-900 shadow-dashboard">
            <p className="font-mono text-xs uppercase tracking-[0.28em] text-rose-500">API Error</p>
            <h2 className="mt-3 font-display text-2xl">Dashboard query failed.</h2>
            <p className="mt-2 text-sm leading-6">
              The read API is reachable enough for the app shell to load, but the dashboard snapshot request failed.
            </p>
          </div>
        ) : !hasData ? (
          <div className="mt-6 rounded-[1.75rem] border border-amber-200 bg-amber-50 p-6 text-amber-950 shadow-dashboard">
            <p className="font-mono text-xs uppercase tracking-[0.28em] text-amber-600">Empty State</p>
            <h2 className="mt-3 font-display text-2xl">No derived dashboard data yet.</h2>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-amber-900/80">
              The frontend is healthy, but the backend has not populated the derived regime, signal, or relationship
              snapshots yet.
            </p>
          </div>
        ) : snapshot ? (
          <div className="mt-6 space-y-10">
            <section>
              <SectionHeading
                eyebrow="Overview"
                title="Macro regime and score stack"
                subtitle="Current state, confidence, and recent regime score movement derived from persisted outputs."
              />
              <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
                <PanelFrame
                  eyebrow="Current Regime"
                  title={snapshot.regime?.regime_label ?? "Unavailable"}
                  subtitle={`Confidence ${(snapshot.regime?.confidence ?? 0).toFixed(2)} | Yield curve ${snapshot.yield_curve_state}`}
                >
                  <div className="grid gap-4 sm:grid-cols-2">
                    {MAJOR_SIGNALS.map((signal) => (
                      <SignalRow
                        key={signal.series}
                        label={signal.label}
                        trend={snapshot.trend_summary[signal.series]?.trend_label ?? "N/A"}
                        value={snapshot.signal_summary[signal.series]?.value}
                        unit={snapshot.signal_summary[signal.series]?.unit}
                      />
                    ))}
                  </div>
                </PanelFrame>

                <PanelFrame
                  eyebrow="Feature Layer"
                  title="Current score stack"
                  subtitle="Four macro scores materialized from the feature and regime engine."
                >
                  <ScoreBars scores={snapshot.scores ?? {}} />
                </PanelFrame>
              </div>

              <div className="mt-6 grid gap-6 xl:grid-cols-[1fr_1fr]">
                <PanelFrame
                  eyebrow="History"
                  title="Signal history"
                  subtitle="Selected market and crypto series with explicit legends and timestamps."
                >
                  <HistoryChart
                    data={buildSignalHistory(snapshot)}
                    series={[...SIGNAL_HISTORY_SERIES]}
                    title="Selected series"
                    unitLabel="Legend shows series, x-axis timestamps are explicit"
                  />
                </PanelFrame>
                <PanelFrame
                  eyebrow="History"
                  title="Score history"
                  subtitle="Recent score movement from persisted regime rows."
                >
                  <HistoryChart
                    data={buildScoreHistory(snapshot)}
                    series={[...SCORE_HISTORY_SERIES]}
                    title="Score history"
                    unitLabel="Integer score levels"
                  />
                </PanelFrame>
              </div>
            </section>

            <section>
              <SectionHeading
                eyebrow="Signals"
                title="Signal layer"
                subtitle="Latest signal snapshots grouped by the categories that feed the feature and regime engines."
              />
              <SignalGroups snapshot={snapshot} />
            </section>

            <section>
              <SectionHeading
                eyebrow="Relationships"
                title="Relationship summary"
                subtitle="Current zero-lag relationships, lagged pairs, and anomaly detection derived from persisted relationship history."
              />
              <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
                <PanelFrame
                  eyebrow="Top Relationships"
                  title="Current relationship lattice"
                  subtitle={`Last relationship snapshot ${formatTimestamp(snapshot.freshness.latest_relationship_ts)}`}
                >
                  <RelationshipTable rows={snapshot.top_relationships} />
                </PanelFrame>
                <PanelFrame
                  eyebrow="Lead-Lag"
                  title="Lagged signals"
                  subtitle="The strongest nonzero lag relationships currently persisted in DuckDB."
                >
                  <RelationshipTable rows={snapshot.lead_lag_relationships} compact />
                </PanelFrame>
              </div>

              <div className="mt-6 grid gap-3">
                {snapshot.anomalies.length ? (
                  snapshot.anomalies.map((anomaly) => (
                    <div
                      key={`${anomaly.series_x}-${anomaly.series_y}-${anomaly.current_window_days}`}
                      className="rounded-[1.5rem] border border-rose-200 bg-rose-50 px-5 py-4"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="font-display text-lg text-rose-950">
                          {anomaly.series_x} vs {anomaly.series_y}
                        </div>
                        <StatusBadge label={anomaly.anomaly_type} tone="error" />
                      </div>
                      <p className="mt-3 text-sm leading-6 text-rose-900/85">
                        Historical state {anomaly.historical_state} degraded to {anomaly.current_state} across the{" "}
                        {anomaly.current_window_days}d window.
                      </p>
                    </div>
                  ))
                ) : (
                  <div className="rounded-[1.5rem] border border-slate-200 bg-white/80 px-5 py-4 text-sm text-slate-500">
                    No anomalies detected in the current snapshot.
                  </div>
                )}
              </div>
            </section>

            <section>
              <SectionHeading
                eyebrow="Alerts"
                title="Alert layer"
                subtitle="This panel reflects alert-engine output when available and degrades gracefully while issue #15 remains open."
              />
              <AlertSummaryPanel summary={snapshot.alert_summary} />
            </section>
          </div>
        ) : null}
      </div>
    </main>
  );
}
