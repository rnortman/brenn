import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.BRENN_E2E_BASE_URL;
if (!baseURL) {
  throw new Error(
    "BRENN_E2E_BASE_URL must be set (e.g. http://127.0.0.1:3100); the `make e2e` target exports it.",
  );
}

// storageState is written by global-setup.ts (a registered+logged-in session)
// and consumed by every spec, so tests start already authenticated.
export const STORAGE_STATE = ".auth/user.json";

export default defineConfig({
  testDir: "./tests",
  // Specs are deliberately sequential and share bus state (retained-ring replay
  // is state persistence across specs), so no parallelism.
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  // Per-assertion budgets (CHAIN_TIMEOUT in the specs) are 20s; the multi-step
  // specs chain two navigations plus several such assertions, so the overall
  // test timeout must dominate that worst case rather than sum-clip at the 30s
  // default.
  timeout: 90_000,
  reporter: "list",
  globalSetup: "./global-setup.ts",
  use: {
    baseURL,
    storageState: STORAGE_STATE,
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        // The stale-build spec injects a bogus build id by intercepting the
        // document request and re-serving it via route.fulfill(). Chrome then
        // classifies that fulfilled document as coming from an unknown/public
        // IP address space, so the page's subsequent loopback (127.0.0.1)
        // WebSocket becomes a public->local request that Local Network Access
        // blocks (net::ERR_BLOCKED_BY_LOCAL_NETWORK_ACCESS_CHECKS) before it
        // reaches the server — an artifact of the injection, not real behavior
        // (normal loads are loopback->loopback; prod is same-origin wss://).
        // Disabling the check lets the intended stale-build handshake happen.
        launchOptions: {
          args: ["--disable-features=LocalNetworkAccessChecks"],
        },
      },
    },
  ],
});
