FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN pip install --no-cache-dir uv==0.11.19 \
    && uv sync --frozen --no-dev --no-cache --no-editable

ENTRYPOINT ["/app/.venv/bin/bet-pipeline"]
