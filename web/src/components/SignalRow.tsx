type SignalRowProps = {
  label: string;
  trend: string;
  value?: number;
  unit?: string;
};

const trendTone: Record<string, string> = {
  UP: "text-emerald-700 bg-emerald-100",
  DOWN: "text-rose-700 bg-rose-100",
  SIDEWAYS: "text-amber-800 bg-amber-100",
  N_A: "text-slate-500 bg-slate-100",
};

export function SignalRow({ label, trend, value, unit }: SignalRowProps) {
  const tone = trendTone[trend] ?? trendTone.N_A;
  const formattedValue =
    value === undefined || Number.isNaN(value)
      ? "N/A"
      : `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}${unit ? ` ${unit}` : ""}`;

  return (
    <div className="flex items-center justify-between rounded-2xl border border-slate-200/70 bg-slate-50/70 px-4 py-3">
      <div>
        <div className="font-display text-sm font-medium text-slate-900">{label}</div>
        <div className="font-mono text-xs uppercase tracking-[0.22em] text-slate-500">{formattedValue}</div>
      </div>
      <span className={`rounded-full px-3 py-1 font-mono text-xs tracking-[0.24em] ${tone}`}>
        {trend === "N/A" ? "NO DATA" : trend}
      </span>
    </div>
  );
}
