type StatusBadgeProps = {
  label: string;
  tone?: "fresh" | "stale" | "error" | "neutral" | "unavailable";
};

const toneClass: Record<NonNullable<StatusBadgeProps["tone"]>, string> = {
  fresh: "bg-emerald-100 text-emerald-800",
  stale: "bg-amber-100 text-amber-900",
  error: "bg-rose-100 text-rose-800",
  neutral: "bg-slate-100 text-slate-700",
  unavailable: "bg-slate-200 text-slate-700",
};

export function StatusBadge({ label, tone = "neutral" }: StatusBadgeProps) {
  return (
    <span className={`inline-flex rounded-full px-3 py-1 font-mono text-[0.68rem] uppercase tracking-[0.22em] ${toneClass[tone]}`}>
      {label}
    </span>
  );
}
