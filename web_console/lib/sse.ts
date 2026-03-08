"use client";

import { useEffect, useMemo, useRef, useState } from "react";

export type StreamState = "connecting" | "open" | "error" | "closed";

type UseSseRefreshResult = {
  state: StreamState;
  lastEventAt: string | null;
  error: string | null;
};

type UseSseRefreshOptions = {
  events?: string[];
};

export function useSseRefresh(
  url: string,
  onEvent: () => void | Promise<void>,
  options: UseSseRefreshOptions = {}
): UseSseRefreshResult {
  const [state, setState] = useState<StreamState>("connecting");
  const [lastEventAt, setLastEventAt] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const lastTriggerAt = useRef(0);
  const events = useMemo(() => options.events || [], [options.events]);

  useEffect(() => {
    const source = new EventSource(url);
    setState("connecting");

    const trigger = () => {
      const now = Date.now();
      if (now - lastTriggerAt.current < 1200) {
        return;
      }
      lastTriggerAt.current = now;
      setLastEventAt(new Date().toISOString());
      setError(null);
      void onEvent();
    };

    source.onopen = () => {
      setState("open");
      setError(null);
    };

    const handlers: Array<{ name: string; handler: EventListener }> = [];
    for (const name of events) {
      const handler: EventListener = () => trigger();
      source.addEventListener(name, handler);
      handlers.push({ name, handler });
    }

    source.addEventListener("error", () => {
      setState("error");
      setError("SSE 通道异常，已回退到轮询。");
    });
    source.addEventListener("heartbeat", () => {
      setState("open");
    });

    return () => {
      for (const { name, handler } of handlers) {
        source.removeEventListener(name, handler);
      }
      source.close();
      setState("closed");
    };
  }, [url, onEvent, events]);

  return { state, lastEventAt, error };
}
