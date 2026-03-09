type SectionHeadingProps = {
  eyebrow: string;
  title: string;
  subtitle: string;
};

export function SectionHeading({ eyebrow, title, subtitle }: SectionHeadingProps) {
  return (
    <div className="mb-5 flex flex-col gap-1">
      <span className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-slate-500">{eyebrow}</span>
      <h2 className="font-display text-2xl font-semibold tracking-tight text-slate-950">{title}</h2>
      <p className="max-w-3xl text-sm leading-6 text-slate-600">{subtitle}</p>
    </div>
  );
}
