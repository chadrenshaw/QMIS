import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        canvas: "var(--color-canvas)",
        ink: "var(--color-ink)",
        accent: "var(--color-accent)",
        signal: "var(--color-signal)",
        alert: "var(--color-alert)",
        surface: "var(--color-surface)",
        shell: "var(--color-shell)",
      },
      fontFamily: {
        display: ["'Space Grotesk'", "sans-serif"],
        mono: ["'IBM Plex Mono'", "monospace"],
      },
      boxShadow: {
        dashboard: "0 32px 80px rgba(26, 37, 47, 0.14)",
      },
      backgroundImage: {
        grid: "linear-gradient(rgba(14, 35, 43, 0.08) 1px, transparent 1px), linear-gradient(90deg, rgba(14, 35, 43, 0.08) 1px, transparent 1px)",
      },
    },
  },
  plugins: [],
} satisfies Config;
