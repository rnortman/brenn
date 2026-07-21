/**
 * UserSettings — typed frontend settings with localStorage persistence.
 *
 * Loads from localStorage on construction, saves on change, fires
 * `brenn-settings-changed` on `document` with the full settings object.
 *
 * The shape matches the generated UserSettings type from Rust (via ts-rs).
 * Backend persistence is future work; localStorage is the interim store.
 */

import type { UserSettings as UserSettingsShape } from "./generated/UserSettings.js";

/** Frontend-only settings that extend the generated Rust shape.
 *  These are persisted in localStorage but not sent to the backend. */
interface LocalSettings extends UserSettingsShape {
  paneSplitRatio: number;
  /** User's preferred model alias (e.g. "sonnet", "opus"). null = app default. */
  preferredModel: string | null;
}

const STORAGE_KEY = "brenn-settings";

const DEFAULTS: LocalSettings = {
  enterSends: true,
  paneSplitRatio: 0.5,
  preferredModel: null,
};

export class UserSettings {
  private data: LocalSettings;

  constructor() {
    this.data = { ...DEFAULTS };
    this.load();
  }

  get enterSends(): boolean {
    return this.data.enterSends;
  }

  set enterSends(value: boolean) {
    if (this.data.enterSends === value) return;
    this.data.enterSends = value;
    this.save();
    this.notify();
  }

  /** Toggle enterSends and return the new value. */
  toggleEnterSends(): boolean {
    this.enterSends = !this.enterSends;
    return this.enterSends;
  }

  get paneSplitRatio(): number {
    return this.data.paneSplitRatio;
  }

  set paneSplitRatio(value: number) {
    if (this.data.paneSplitRatio === value) return;
    this.data.paneSplitRatio = value;
    this.save();
    this.notify();
  }

  get preferredModel(): string | null {
    return this.data.preferredModel;
  }

  set preferredModel(value: string | null) {
    if (this.data.preferredModel === value) return;
    this.data.preferredModel = value;
    this.save();
    this.notify();
  }

  private load(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as Partial<LocalSettings>;
        if (typeof parsed.enterSends === "boolean") {
          this.data.enterSends = parsed.enterSends;
        }
        if (typeof parsed.paneSplitRatio === "number") {
          this.data.paneSplitRatio = Math.max(0.1, Math.min(0.9, parsed.paneSplitRatio));
        }
        if (typeof parsed.preferredModel === "string" || parsed.preferredModel === null) {
          this.data.preferredModel = parsed.preferredModel;
        }
      }
    } catch {
      // Corrupted or unavailable localStorage — use defaults.
    }
  }

  private save(): void {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(this.data));
    } catch {
      // localStorage full or unavailable — settings won't persist.
    }
  }

  private notify(): void {
    document.dispatchEvent(
      new CustomEvent("brenn-settings-changed", {
        detail: { settings: { ...this.data } },
      }),
    );
  }
}
