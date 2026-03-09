import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type HistoryChartSeries = {
  key: string;
  label: string;
  color: string;
};

type HistoryChartProps = {
  data: Array<Record<string, number | string | null>>;
  series: HistoryChartSeries[];
  title: string;
  unitLabel: string;
};

function formatTickLabel(value: string | number) {
  if (typeof value !== "string") {
    return String(value);
  }
  return value.slice(5, 10);
}

export function HistoryChart({ data, series, title, unitLabel }: HistoryChartProps) {
  if (data.length === 0) {
    return <p className="text-sm text-slate-500">No history available yet.</p>;
  }

  return (
    <div className="rounded-[1.5rem] border border-slate-200/70 bg-slate-50/70 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <h3 className="font-display text-lg text-slate-900">{title}</h3>
          <p className="font-mono text-[0.68rem] uppercase tracking-[0.18em] text-slate-500">{unitLabel}</p>
        </div>
      </div>
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 12, right: 12, left: -12, bottom: 0 }}>
            <CartesianGrid stroke="#d9e2e8" strokeDasharray="4 4" />
            <XAxis dataKey="ts" tickFormatter={formatTickLabel} tick={{ fill: "#405364", fontSize: 12 }} />
            <YAxis tick={{ fill: "#405364", fontSize: 12 }} />
            <Tooltip
              formatter={(value, name) => [`${Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })}`, String(name)]}
              labelFormatter={(label) => `Timestamp ${label}`}
            />
            <Legend />
            {series.map((line) => (
              <Line
                key={line.key}
                type="monotone"
                dataKey={line.key}
                name={line.label}
                stroke={line.color}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
