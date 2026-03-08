"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  DEFAULT_POLLING_CONFIG,
  detectPollingMode,
  type PollingConfig,
  type PollingMode,
  resolvePollingIntervalMs
} from "@/lib/polling";

type UseRealtimeResourceOptions = {
  polling?: Partial<PollingConfig>;
  refreshOnMount?: boolean;
};

type UseRealtimeResourceResult<T> = {
  data: T;
  isRefreshing: boolean;
  error: string | null;
  pollingMode: PollingMode;
  lastRefreshedAt: string | null;
  refreshNow: () => Promise<void>;
};

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export function useRealtimeResource<T>(
  loader: () => Promise<T>,
  initialData: T,
  options: UseRealtimeResourceOptions = {}
): UseRealtimeResourceResult<T> {
  const [data, setData] = useState<T>(initialData);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pollingMode, setPollingMode] = useState<PollingMode>("focused");
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);

  const timerRef = useRef<number | null>(null);
  const inFlightRef = useRef(false);

  const polling = useMemo<PollingConfig>(() => {
    return {
      focusedMs: options.polling?.focusedMs ?? DEFAULT_POLLING_CONFIG.focusedMs,
      blurredMs: options.polling?.blurredMs ?? DEFAULT_POLLING_CONFIG.blurredMs,
      hiddenMs: options.polling?.hiddenMs ?? DEFAULT_POLLING_CONFIG.hiddenMs
    };
  }, [options.polling?.focusedMs, options.polling?.blurredMs, options.polling?.hiddenMs]);

  const refreshNow = useCallback(async () => {
    if (inFlightRef.current) {
      return;
    }
    inFlightRef.current = true;
    setIsRefreshing(true);
    try {
      const next = await loader();
      setData(next);
      setError(null);
      setLastRefreshedAt(new Date().toISOString());
    } catch (cause) {
      setError(toErrorMessage(cause));
    } finally {
      inFlightRef.current = false;
      setIsRefreshing(false);
    }
  }, [loader]);

  useEffect(() => {
    let disposed = false;

    const clearTimer = () => {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };

    const scheduleNext = () => {
      clearTimer();
      if (disposed) {
        return;
      }
      const mode = detectPollingMode();
      setPollingMode(mode);
      const delay = resolvePollingIntervalMs(mode, polling);
      if (delay === null) {
        return;
      }
      timerRef.current = window.setTimeout(async () => {
        await refreshNow();
        scheduleNext();
      }, delay);
    };

    const onAttentionChanged = () => {
      scheduleNext();
    };

    window.addEventListener("focus", onAttentionChanged);
    window.addEventListener("blur", onAttentionChanged);
    document.addEventListener("visibilitychange", onAttentionChanged);

    if (options.refreshOnMount ?? true) {
      void refreshNow().finally(scheduleNext);
    } else {
      scheduleNext();
    }

    return () => {
      disposed = true;
      clearTimer();
      window.removeEventListener("focus", onAttentionChanged);
      window.removeEventListener("blur", onAttentionChanged);
      document.removeEventListener("visibilitychange", onAttentionChanged);
    };
  }, [polling, refreshNow, options.refreshOnMount]);

  return {
    data,
    isRefreshing,
    error,
    pollingMode,
    lastRefreshedAt,
    refreshNow
  };
}
