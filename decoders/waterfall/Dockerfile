FROM docker.io/library/python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY waterfall.py waterfall.sh ./
RUN chmod +x waterfall.sh

ENV PATH="/app/.venv/bin:$PATH"
ENV MPLCONFIGDIR=/opt/mpl
RUN mkdir -p /opt/mpl && \
    python -c "import matplotlib; matplotlib.use('Agg'); \
import matplotlib.pyplot as plt; f=plt.figure(); plt.plot([0,1],[0,1]); plt.close(f)"

ENTRYPOINT ["/app/waterfall.sh"]
