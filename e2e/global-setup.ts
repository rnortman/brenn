import { chromium } from "@playwright/test";
import * as path from "path";
import { STORAGE_STATE } from "./playwright.config";

// Fixed credentials for the hermetic e2e user. The password satisfies the
// backend's 12-char minimum; the username is alphanumeric per the register
// validation rules.
const E2E_USERNAME = "e2e-user";
const E2E_PASSWORD = "e2e-password-12";

/**
 * Registers and logs in a single user through the real auth forms, then saves
 * the authenticated session as Playwright storageState for every spec. No
 * test-only auth backdoor: this exercises POST /auth/register and
 * POST /auth/login exactly as a browser would.
 */
async function globalSetup(): Promise<void> {
  const baseURL = process.env.BRENN_E2E_BASE_URL;
  if (!baseURL) {
    throw new Error("BRENN_E2E_BASE_URL must be set for e2e global setup.");
  }
  const inviteCode = process.env.BRENN_E2E_INVITE;
  if (!inviteCode) {
    throw new Error(
      "BRENN_E2E_INVITE must be set (the `make e2e` target mints and exports it).",
    );
  }

  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({ baseURL });

    // Register — the endpoint auto-logs-in and redirects to "/" on success, or
    // to /auth/register?error=... on failure. Do not follow the redirect so we
    // can assert on the Location.
    const register = await context.request.post("/auth/register", {
      form: {
        invite_code: inviteCode,
        username: E2E_USERNAME,
        password: E2E_PASSWORD,
      },
      maxRedirects: 0,
    });
    const registerLocation = register.headers()["location"];
    if (register.status() !== 303 || registerLocation !== "/") {
      throw new Error(
        `registration failed: status=${register.status()} location=${registerLocation}`,
      );
    }

    // Log in explicitly. Registration above already auto-logs-in and set a
    // session cookie; this second POST is deliberate independent coverage of
    // POST /auth/login (no spec otherwise exercises it) and lands a clean fresh
    // session cookie in the context's jar before storageState is saved.
    const login = await context.request.post("/auth/login", {
      form: {
        username: E2E_USERNAME,
        password: E2E_PASSWORD,
      },
      maxRedirects: 0,
    });
    const loginLocation = login.headers()["location"];
    if (login.status() !== 303 || loginLocation !== "/") {
      throw new Error(
        `login failed: status=${login.status()} location=${loginLocation}`,
      );
    }

    // Persist the authenticated session where the config points every spec.
    const storagePath = path.join(__dirname, STORAGE_STATE);
    await context.storageState({ path: storagePath });

    await context.close();
  } finally {
    await browser.close();
  }
}

export default globalSetup;
