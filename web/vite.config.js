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
export default defineConfig({
    plugins: [react()],
    server: {
        port: 5173,
        proxy: Object.fromEntries(apiProxyPaths.map((path) => [
            path,
            {
                target: "http://127.0.0.1:8000",
                changeOrigin: true,
            },
        ])),
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
