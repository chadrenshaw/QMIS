import type { DashboardAlertSummary } from "../api/queries";
import { StatusBadge } from "./StatusBadge";

type AlertSummaryPanelProps = {
  summary: DashboardAlertSummary;
};

export function AlertSummaryPanel({ summary }: AlertSummaryPanelProps) {
  const tone = summary.status === "unavailable" ? "unavailable" : summary.status === "stale" ? "stale" : "fresh";

  return (
    <div className="space-y-4 rounded-[1.5rem] border border-slate-200/80 bg-white/80 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="font-display text-lg text-slate-950">Alert summary</h3>
          <p className="mt-1 text-sm text-slate-600">{summary.message}</p>
        </div>
        <StatusBadge label={summary.status} tone={tone} />
      </div>
      {summary.updated_at ? (
        <p className="font-mono text-[0.68rem] uppercase tracking-[0.18em] text-slate-500">
          Last updated {summary.updated_at}
        </p>
      ) : (
        <p className="font-mono text-[0.68rem] uppercase tracking-[0.18em] text-slate-500">Awaiting alert engine output</p>
      )}
    </div>
  );
}
