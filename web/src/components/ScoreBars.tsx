import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";

type ScoreBarsProps = {
  scores: Record<string, number>;
};

const palette = ["#cf5f18", "#227c9d", "#2f855a", "#9f1239"];

export function ScoreBars({ scores }: ScoreBarsProps) {
  const data = [
    { name: "Inflation", value: scores.inflation_score ?? 0 },
    { name: "Growth", value: scores.growth_score ?? 0 },
    { name: "Liquidity", value: scores.liquidity_score ?? 0 },
    { name: "Risk", value: scores.risk_score ?? 0 },
  ];

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
          <CartesianGrid strokeDasharray="4 4" stroke="#d9e2e8" />
          <XAxis dataKey="name" tick={{ fill: "#405364", fontSize: 12 }} />
          <YAxis allowDecimals={false} tick={{ fill: "#405364", fontSize: 12 }} />
          <Bar dataKey="value" radius={[10, 10, 0, 0]}>
            {data.map((_, index) => (
              <Cell key={index} fill={palette[index % palette.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
