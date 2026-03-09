import react from "@vitejs/plugin-react-swc";
import { defineConfig } from "vitest/config";

const apiProxyPaths = [
  "/health",
  "/regime",
  "/signals",
  "/relationships",
  "/anomalies",
  "/dashboard",
];

const apiProxyTarget = process.env.QMIS_API_PROXY_TARGET?.trim();

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: apiProxyTarget
      ? Object.fromEntries(
          apiProxyPaths.map((path) => [
            path,
            {
              target: apiProxyTarget,
              changeOrigin: true,
            },
          ]),
        )
      : undefined,
  },
  preview: {
    port: 4173,
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    css: true,
  },
});
