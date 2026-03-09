import { devMockDashboard, devMockHealth } from "./devMocks";

type ApiRequestPlan =
  | { mode: "mock"; path: string }
  | { mode: "fetch"; url: string };

const DEV_MOCK_RESPONSES: Record<string, unknown> = {
  "/health": devMockHealth,
  "/dashboard": devMockDashboard,
};

function cloneMock<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function getApiRequestPlan(
  path: string,
  options: {
    isDev?: boolean;
    apiBaseUrl?: string;
  } = {},
): ApiRequestPlan {
  const isDev = options.isDev ?? import.meta.env.DEV;
  const apiBaseUrl = (options.apiBaseUrl ?? import.meta.env.VITE_QMIS_API_BASE_URL ?? "").trim();

  if (isDev && !apiBaseUrl && path in DEV_MOCK_RESPONSES) {
    return { mode: "mock", path };
  }

  if (apiBaseUrl) {
    return { mode: "fetch", url: `${apiBaseUrl.replace(/\/$/, "")}${path}` };
  }

  return { mode: "fetch", url: path };
}

export async function fetchJson<T>(path: string): Promise<T> {
  const requestPlan = getApiRequestPlan(path);
  if (requestPlan.mode === "mock") {
    return cloneMock(DEV_MOCK_RESPONSES[requestPlan.path] as T);
  }

  const response = await fetch(requestPlan.url, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed for ${requestPlan.url}: ${response.status}`);
  }

  return (await response.json()) as T;
}
