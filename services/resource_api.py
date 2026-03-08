from __future__ import annotations

import asyncio
import argparse
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from aiohttp import web

from services.artifact_store import ArtifactStore
from services.events_read_model import EventsReadModel
from services.governance_read_model import GovernanceReadModel
from services.run_store import RunStore
from services.store_adapter import resolve_db_path
from services.telegram_store import TelegramTaskStore

RESOURCE_API_SCHEMA_VERSION = "upgrade12.resource_api.v2"
RESOURCE_VERSIONS: dict[str, str] = {
    "runs": "upgrade12.resource.runs.v1",
    "alerts": "upgrade12.resource.alerts.v1",
    "governance": "upgrade12.resource.governance.v1",
    "monitors": "upgrade12.resource.monitors.v1",
    "events": "upgrade12.resource.events.v1",
    "evidence": "upgrade12.resource.evidence.v1",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_db_path(explicit: str | Path | None = None) -> Path:
    return resolve_db_path(explicit)


def _envelope(*, data: Any, resource: str) -> dict[str, Any]:
    return {
        "schema_version": RESOURCE_API_SCHEMA_VERSION,
        "resource": resource,
        "resource_version": RESOURCE_VERSIONS[resource],
        "generated_at": _utc_now_iso(),
        "data": data,
    }


def _parse_limit(raw: str | None, *, default: int, cap: int = 500) -> int:
    try:
        parsed = int(raw or default)
    except Exception:
        parsed = default
    return max(1, min(parsed, cap))


class ResourceAPIService:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        evidence_dir: str | Path = "docs/evidence",
        governance: GovernanceReadModel | None = None,
        events: EventsReadModel | None = None,
        run_store: RunStore | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self._store = store
        self._runs = run_store if run_store is not None else RunStore(db_path=store.db_path)
        self._artifacts = artifact_store if artifact_store is not None else ArtifactStore(evidence_dir=evidence_dir)
        self._governance = governance if governance is not None else GovernanceReadModel(store)
        self._events = events if events is not None else EventsReadModel(store.db_path)

    def list_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return [row.model_dump(mode="python") for row in self._runs.list_runs(limit=limit)]

    def get_run(self, *, run_id: str) -> dict[str, Any] | None:
        report = self._runs.get_run(run_id=run_id)
        if report is None:
            return None
        return report.model_dump(mode="python")

    def list_alerts(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return [row.model_dump(mode="python") for row in self._runs.list_alerts(limit=limit)]

    def list_governance(self, *, limit: int = 200) -> dict[str, Any]:
        snapshot = self._governance.build_snapshot(limit=limit)
        return asdict(snapshot)

    def list_monitors(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return [row.model_dump(mode="python") for row in self._runs.list_monitors(limit=limit)]

    def list_events(self, *, since: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self._events.list_events(since=since, limit=limit)

    def list_evidence(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return [row.model_dump(mode="python") for row in self._artifacts.list_evidence(limit=limit)]


RESOURCE_SERVICE_KEY: web.AppKey[ResourceAPIService] = web.AppKey("resource_service", ResourceAPIService)


def create_resource_api_app(service: ResourceAPIService) -> web.Application:
    app = web.Application()
    app[RESOURCE_SERVICE_KEY] = service

    async def health(_: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "schema_version": RESOURCE_API_SCHEMA_VERSION,
                "resource_versions": RESOURCE_VERSIONS,
            }
        )

    async def list_runs(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=50)
        return web.json_response(_envelope(data=svc.list_runs(limit=limit), resource="runs"))

    async def get_run(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        run_id = str(request.match_info.get("run_id", "")).strip()
        payload = svc.get_run(run_id=run_id)
        if payload is None:
            return web.json_response(_envelope(data={"error": "run_not_found", "run_id": run_id}, resource="runs"), status=404)
        return web.json_response(_envelope(data=payload, resource="runs"))

    async def list_alerts(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=100)
        return web.json_response(_envelope(data=svc.list_alerts(limit=limit), resource="alerts"))

    async def list_governance(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=200)
        return web.json_response(_envelope(data=svc.list_governance(limit=limit), resource="governance"))

    async def list_monitors(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=200)
        return web.json_response(_envelope(data=svc.list_monitors(limit=limit), resource="monitors"))

    async def list_events(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=200)
        since = request.query.get("since")
        return web.json_response(_envelope(data=svc.list_events(since=since, limit=limit), resource="events"))

    async def list_evidence(request: web.Request) -> web.Response:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=100)
        return web.json_response(_envelope(data=svc.list_evidence(limit=limit), resource="evidence"))

    async def _stream_resource(
        *,
        request: web.Request,
        resource: str,
        loader: Callable[..., list[dict[str, Any]]],
        limit_default: int,
        since_supported: bool = False,
    ) -> web.StreamResponse:
        svc = request.app[RESOURCE_SERVICE_KEY]
        limit = _parse_limit(request.query.get("limit"), default=limit_default)
        since = request.query.get("since") if since_supported else None

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)

        async def write_event(event: str, payload: dict[str, Any]) -> None:
            message = f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
            await response.write(message.encode("utf-8"))

        await write_event(
            "ready",
            {
                "schema_version": RESOURCE_API_SCHEMA_VERSION,
                "resource": resource,
                "resource_version": RESOURCE_VERSIONS[resource],
                "generated_at": _utc_now_iso(),
            },
        )

        last_token: str | None = None
        # Keep stream open for up to ~30 minutes, browser can reconnect.
        try:
            for _ in range(900):
                transport = request.transport
                if transport is None or transport.is_closing():
                    break
                if since_supported:
                    rows = loader(svc, since=since, limit=limit)
                else:
                    rows = loader(svc, limit=limit)
                head = rows[0] if rows else None
                if since_supported:
                    token = (
                        f"{head.get('event_id','')}:{head.get('ts','')}"
                        if isinstance(head, dict)
                        else ""
                    )
                else:
                    token = (
                        f"{head.get('event_id','')}:{head.get('updated_at','')}:{head.get('status','')}"
                        if isinstance(head, dict)
                        else ""
                    )
                if token and token != last_token:
                    last_token = token
                    if since_supported and isinstance(head, dict):
                        head_ts = str(head.get("ts", "")).strip()
                        if head_ts:
                            since = head_ts
                    await write_event(
                        resource,
                        _envelope(
                            data=rows,
                            resource=resource,
                        ),
                    )
                else:
                    await write_event("heartbeat", {"generated_at": _utc_now_iso(), "resource": resource})
                await asyncio.sleep(2)
        except (ConnectionResetError, RuntimeError):
            pass

        return response

    async def stream_events(request: web.Request) -> web.StreamResponse:
        return await _stream_resource(
            request=request,
            resource="events",
            loader=lambda svc, since=None, limit=200: svc.list_events(since=since, limit=limit),
            limit_default=200,
            since_supported=True,
        )

    async def stream_alerts(request: web.Request) -> web.StreamResponse:
        return await _stream_resource(
            request=request,
            resource="alerts",
            loader=lambda svc, limit=100: svc.list_alerts(limit=limit),
            limit_default=100,
            since_supported=False,
        )

    app.add_routes(
        [
            web.get("/healthz", health),
            web.get("/api/runs", list_runs),
            web.get("/api/runs/{run_id}", get_run),
            web.get("/api/alerts", list_alerts),
            web.get("/api/governance", list_governance),
            web.get("/api/monitors", list_monitors),
            web.get("/api/events", list_events),
            web.get("/api/events/stream", stream_events),
            web.get("/api/alerts/stream", stream_alerts),
            web.get("/api/evidence", list_evidence),
        ]
    )
    return app


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upgrade10 realtime resource API service")
    parser.add_argument("--host", default=os.getenv("RESOURCE_API_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("RESOURCE_API_PORT", "8765")))
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--evidence-dir", default=os.getenv("RESOURCE_API_EVIDENCE_DIR", "docs/evidence"))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    db_path = _resolve_db_path(args.db_path)
    store = TelegramTaskStore(db_path)
    service = ResourceAPIService(store=store, evidence_dir=args.evidence_dir)
    app = create_resource_api_app(service)
    web.run_app(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
