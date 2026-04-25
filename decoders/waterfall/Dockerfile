FROM docker.io/library/python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY waterfall.py waterfall.sh ./
RUN chmod +x waterfall.sh

ENV PATH="/app/.venv/bin:$PATH"
ENV MPLCONFIGDIR=/tmp/mpl

ENTRYPOINT ["/app/waterfall.sh"]
