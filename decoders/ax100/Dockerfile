ARG GHCR_REGISTRY=ghcr.io
ARG GITHUB_REPOSITORY=xarantolus/groundstation
ARG TAG=latest

FROM ${GHCR_REGISTRY}/${GITHUB_REPOSITORY}/gs:${TAG}

# libsocketcan-devel is the link-time dep for libcsp_py3's socketcan backend.
RUN --mount=type=cache,target=/var/cache/dnf \
	dnf install -y --setopt=keepcache=1 libsocketcan-devel

RUN --mount=type=cache,target=/root/.cache/pip \
	pip3 install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
	uv sync --no-install-project

COPY csp_helpers.py decode.py deframe.py /app/

COPY ax100_decoder.grc /tmp/ax100_decoder.grc
RUN set -e; cd /tmp; \
	if grcc ax100_decoder.grc; then \
		mv ax100_decoder.py /usr/local/bin/ax100_decoder.py; \
	else \
		printf '#!/bin/sh\necho "ax100_decoder.grc failed to compile; skipping GNU Radio stage" >&2\n' \
			> /usr/local/bin/ax100_decoder.py; \
	fi; \
	chmod +x /usr/local/bin/ax100_decoder.py

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

CMD ["/usr/local/bin/entrypoint.sh"]
