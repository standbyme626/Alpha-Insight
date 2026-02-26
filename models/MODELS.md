# Model Inventory

## What the code actually needs

1. Remote planner/chat model via OpenAI-compatible API:
   - Env: `OPENAI_API_KEY`, `OPENAI_API_BASE`, `OPENAI_MODEL_NAME`
   - Current `.env`: `OPENAI_MODEL_NAME=qwen3-32b`
2. Optional fallback planner:
   - `ENABLE_LOCAL_FALLBACK=true` uses a heuristic rule path in code
   - No local LLM weights are required for this fallback

## Local download status

- This repo does not load local model files from `models/` at runtime.
- Therefore there are no mandatory local model weights to download.
- `models/` is created for future local-model extension and artifact management.

## Availability check (2026-02-26)

- Pulled provider model list into `models/provider_models_2026-02-26.json`.
- Verified chat-completions call succeeds for:
  - `qwen3-32b`
  - `qwen3.5-27b`
