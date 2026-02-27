from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.telegram_store import TelegramTaskStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Telegram Phase D run report")
    parser.add_argument("--db-path", default="storage/telegram_gateway.db")
    parser.add_argument("--output", default="docs/evidence/telegram_phase_d_run_report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store = TelegramTaskStore(Path(args.db_path))
    report = store.build_phase_d_run_report()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

