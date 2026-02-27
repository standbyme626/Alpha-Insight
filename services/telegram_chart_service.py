from __future__ import annotations

from pathlib import Path
from typing import Any


class TelegramChartService:
    def __init__(self, *, max_payload_bytes: int = 5 * 1024 * 1024):
        self._max_payload_bytes = max(1, int(max_payload_bytes))

    def extract_chart_path(self, result: dict[str, Any]) -> Path | None:
        direct = result.get("artifact_png")
        if isinstance(direct, str) and direct.strip():
            return Path(direct.strip())

        sandbox_artifacts = result.get("sandbox_artifacts")
        if not isinstance(sandbox_artifacts, dict):
            return None
        stdout = sandbox_artifacts.get("stdout")
        if not isinstance(stdout, str) or not stdout.strip():
            return None

        for line in stdout.splitlines():
            row = line.strip()
            if row.startswith("ARTIFACT_PNG="):
                _, value = row.split("=", 1)
                if value.strip():
                    return Path(value.strip())
        return None

    def ensure_chart_within_limit(self, chart_path: Path | None) -> tuple[Path | None, int | None, str | None]:
        if chart_path is None:
            return None, None, "chart_missing"
        if not chart_path.exists() or not chart_path.is_file():
            return None, None, "chart_not_found"

        size = int(chart_path.stat().st_size)
        if size > self._max_payload_bytes:
            return None, size, "chart_payload_too_large"
        return chart_path, size, None
