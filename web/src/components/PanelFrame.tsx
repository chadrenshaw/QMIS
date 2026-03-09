import type { ReactNode } from "react";

type PanelFrameProps = {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  children: ReactNode;
};

export function PanelFrame({ eyebrow, title, subtitle, children }: PanelFrameProps) {
  return (
    <section className="rounded-[1.75rem] border border-white/50 bg-white/85 p-5 shadow-dashboard backdrop-blur">
      <div className="mb-4 flex flex-col gap-1">
        {eyebrow ? (
          <span className="font-mono text-[0.7rem] uppercase tracking-[0.32em] text-slate-500">
            {eyebrow}
          </span>
        ) : null}
        <h2 className="font-display text-xl font-semibold text-slate-900">{title}</h2>
        {subtitle ? <p className="text-sm text-slate-600">{subtitle}</p> : null}
      </div>
      {children}
    </section>
  );
}
