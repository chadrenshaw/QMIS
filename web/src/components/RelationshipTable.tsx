import type { DashboardRelationship } from "../api/queries";

type RelationshipTableProps = {
  rows: DashboardRelationship[];
  compact?: boolean;
};

export function RelationshipTable({ rows, compact = false }: RelationshipTableProps) {
  if (rows.length === 0) {
    return <p className="text-sm text-slate-500">No relationship data available yet.</p>;
  }

  return (
    <div className="overflow-hidden rounded-2xl border border-slate-200/80">
      <table className="min-w-full divide-y divide-slate-200 text-sm">
        <thead className="bg-slate-100/80 font-mono text-[0.68rem] uppercase tracking-[0.18em] text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Pair</th>
            <th className="px-4 py-3 text-right">Window</th>
            {!compact ? <th className="px-4 py-3 text-right">Lag</th> : null}
            <th className="px-4 py-3 text-right">Corr</th>
            <th className="px-4 py-3 text-left">State</th>
            <th className="px-4 py-3 text-left">Confidence</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-200 bg-white/80">
          {rows.map((row) => (
            <tr key={`${row.series_x}-${row.series_y}-${row.window_days}-${row.lag_days}`}>
              <td className="px-4 py-3 text-slate-900">
                {row.series_x} <span className="text-slate-400">vs</span> {row.series_y}
              </td>
              <td className="px-4 py-3 text-right text-slate-600">{row.window_days}d</td>
              {!compact ? <td className="px-4 py-3 text-right text-slate-600">{row.lag_days}d</td> : null}
              <td className="px-4 py-3 text-right font-mono text-slate-700">
                {row.correlation.toFixed(2)}
              </td>
              <td className="px-4 py-3 text-slate-700">{row.relationship_state}</td>
              <td className="px-4 py-3 text-slate-700">{row.confidence_label}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
