# Standalone image containing only gr-satellites (https://github.com/daniestevez/gr-satellites)
# Built as a GNU Radio out-of-tree module, following the same builder/runner
# pattern used for the SatDump image in this repo.
ARG DEBIAN_IMAGE_TAG=bookworm-slim
FROM debian:${DEBIAN_IMAGE_TAG} AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG CMAKE_BUILD_PARALLEL_LEVEL=""
ENV TZ=Etc/UTC

WORKDIR /usr/local/src/

# Cache apt package downloads
RUN --mount=type=cache,target=/var/cache/apt \
	--mount=type=cache,target=/var/lib/apt \
	apt -y update && \
	apt -y upgrade && \
	apt install --no-install-recommends -y \
	ca-certificates git build-essential cmake pkg-config swig \
	gnuradio-dev libvolk2-dev liborc-0.4-dev \
	python3-dev python3-pip

# construct/requests are Python deps used by gr-satellites at runtime,
# but also needed at build time for the cmake test discovery step.
RUN --mount=type=cache,target=/root/.cache/pip \
	pip3 install --no-cache-dir --break-system-packages construct requests

# main branch tracks current GNU Radio (3.10/3.11); pin a release tag here
# instead of `--depth 1` on the default branch if you want reproducible builds.
RUN git clone --depth 1 https://github.com/daniestevez/gr-satellites.git

WORKDIR /usr/local/src/gr-satellites

RUN cmake -B build \
	-DCMAKE_BUILD_TYPE=Release \
	-DCMAKE_INSTALL_PREFIX=/usr \
	-Wno-dev && \
	cmake --build build -j${CMAKE_BUILD_PARALLEL_LEVEL:-$(nproc)} && \
	DESTDIR=/tmp/install cmake --install build

ARG DEBIAN_IMAGE_TAG=bookworm-slim
FROM debian:${DEBIAN_IMAGE_TAG} AS runner

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Cache apt package downloads for runner dependencies
RUN --mount=type=cache,target=/var/cache/apt \
	--mount=type=cache,target=/var/lib/apt \
	apt -y update && \
	apt -y upgrade && \
	apt install --no-install-recommends -y \
	gnuradio libvolk2.5 liborc-0.4-0 \
	python3 python3-pip

RUN --mount=type=cache,target=/root/.cache/pip \
	pip3 install --no-cache-dir --break-system-packages construct requests

# Copy the gr-satellites out-of-tree module + Python package built above
COPY --from=builder /tmp/install/ /
RUN ldconfig

ENV PYTHONPATH=/usr/lib/python3/dist-packages

# Baked-in gr-satellites settings (disables telemetry submission); entrypoint.sh
# copies this into $HOME/.gr_satellites/config.ini at runtime.
COPY gr-satellites-config.ini /etc/groundstation/gr-satellites-config.ini

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /srv
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
