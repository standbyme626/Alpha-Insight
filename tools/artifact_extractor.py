"""Helpers for transforming sandbox artifacts into transport payloads."""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path


_ALLOWED_SUFFIXES = {".png", ".html", ".pdf"}


def _guess_mime(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def encode_artifact(path: str) -> dict[str, str]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"artifact not found: {path}")
    if file_path.suffix.lower() not in _ALLOWED_SUFFIXES:
        raise ValueError(f"unsupported artifact type: {file_path.suffix}")

    encoded = base64.b64encode(file_path.read_bytes()).decode("ascii")
    return {
        "name": file_path.name,
        "path": str(file_path),
        "mime": _guess_mime(file_path),
        "base64": encoded,
    }


def build_transfer_payload(paths: list[str]) -> list[dict[str, str]]:
    return [encode_artifact(path) for path in paths]
