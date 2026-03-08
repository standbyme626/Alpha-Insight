import { NextRequest } from "next/server";

import { listAlerts } from "@/lib/resources";
import { RESOURCE_API_SCHEMA_VERSION, RESOURCE_VERSIONS } from "@/lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function encodeSse(event: string, payload: unknown): Uint8Array {
  const line = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  return new TextEncoder().encode(line);
}

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "100");
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.floor(limit)) : 100;
  let lastToken = "";
  let timer: ReturnType<typeof setInterval> | undefined;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const push = async () => {
        try {
          const rows = await listAlerts(normalizedLimit);
          const first = rows[0];
          const token = first ? `${first.event_id}:${first.updated_at}:${first.status}` : "";
          if (token && token !== lastToken) {
            lastToken = token;
            controller.enqueue(
              encodeSse("alerts", {
                schema_version: RESOURCE_API_SCHEMA_VERSION,
                resource: "alerts",
                resource_version: RESOURCE_VERSIONS.alerts,
                generated_at: new Date().toISOString(),
                data: rows
              })
            );
          } else {
            controller.enqueue(encodeSse("heartbeat", { resource: "alerts", generated_at: new Date().toISOString() }));
          }
        } catch (error) {
          controller.enqueue(
            encodeSse("error", {
              resource: "alerts",
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
