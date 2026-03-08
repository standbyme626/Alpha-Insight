export type PollingMode = "focused" | "blurred" | "hidden";

export type PollingConfig = {
  focusedMs: number;
  blurredMs: number;
  hiddenMs: number | null;
};

export const DEFAULT_POLLING_CONFIG: PollingConfig = {
  focusedMs: 3000,
  blurredMs: 15000,
  hiddenMs: null
};

export function detectPollingMode(): PollingMode {
  if (typeof document === "undefined") {
    return "focused";
  }
  if (document.visibilityState === "hidden") {
    return "hidden";
  }
  if (typeof document.hasFocus === "function" && !document.hasFocus()) {
    return "blurred";
  }
  return "focused";
}

export function resolvePollingIntervalMs(mode: PollingMode, config: PollingConfig): number | null {
  if (mode === "hidden") {
    return config.hiddenMs;
  }
  if (mode === "blurred") {
    return config.blurredMs;
  }
  return config.focusedMs;
}
