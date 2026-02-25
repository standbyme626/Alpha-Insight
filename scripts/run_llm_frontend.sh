#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=/workspace
exec streamlit run ui/llm_frontend.py --server.address 0.0.0.0 --server.port 8501
