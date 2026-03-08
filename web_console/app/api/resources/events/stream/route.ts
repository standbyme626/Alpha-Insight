import { NextRequest } from "next/server";

import { listEvents } from "@/lib/resources";
import { RESOURCE_API_SCHEMA_VERSION, RESOURCE_VERSIONS } from "@/lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function encodeSse(event: string, payload: unknown): Uint8Array {
  const line = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  return new TextEncoder().encode(line);
}

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "200");
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.floor(limit)) : 200;
  let since = request.nextUrl.searchParams.get("since");
  let lastToken = "";
  let timer: ReturnType<typeof setInterval> | undefined;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const push = async () => {
        try {
          const rows = await listEvents({ limit: normalizedLimit, since });
          const first = rows[0];
          const token = first ? `${first.event_id}:${first.ts}` : "";
          if (token && token !== lastToken) {
            lastToken = token;
            since = first.ts;
            controller.enqueue(
              encodeSse("events", {
                schema_version: RESOURCE_API_SCHEMA_VERSION,
                resource: "events",
                resource_version: RESOURCE_VERSIONS.events,
                generated_at: new Date().toISOString(),
                data: rows
              })
            );
          } else {
            controller.enqueue(encodeSse("heartbeat", { resource: "events", generated_at: new Date().toISOString() }));
          }
        } catch (error) {
          controller.enqueue(
            encodeSse("error", {
              resource: "events",
              message: error instanceof Error ? error.message : String(error),
              generated_at: new Date().toISOString()
            })
          );
        }
      };

      void push();
      timer = setInterval(() => {
        void push();
      }, 2000);

      request.signal.addEventListener("abort", () => {
        if (timer) {
          clearInterval(timer);
          timer = undefined;
        }
        controller.close();
      });
    },
    cancel() {
      if (timer) {
        clearInterval(timer);
        timer = undefined;
      }
    }
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    }
  });
}
