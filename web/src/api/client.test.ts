import { describe, expect, it } from "vitest";

import { getApiRequestPlan } from "./client";

describe("getApiRequestPlan", () => {
  it("uses local dev mocks when no QMIS API base URL is configured", () => {
    expect(getApiRequestPlan("/health", { isDev: true, apiBaseUrl: "" })).toEqual({
      mode: "mock",
      path: "/health",
    });
    expect(getApiRequestPlan("/dashboard", { isDev: true, apiBaseUrl: "" })).toEqual({
      mode: "mock",
      path: "/dashboard",
    });
  });

  it("uses explicit API base URL when configured", () => {
    expect(getApiRequestPlan("/dashboard", { isDev: true, apiBaseUrl: "http://127.0.0.1:8787" })).toEqual({
      mode: "fetch",
      url: "http://127.0.0.1:8787/dashboard",
    });
  });

  it("uses same-origin requests outside dev mode", () => {
    expect(getApiRequestPlan("/dashboard", { isDev: false, apiBaseUrl: "" })).toEqual({
      mode: "fetch",
      url: "/dashboard",
    });
  });
});
