# This file is essentially a copy of the Dockerfile from the SatDump repository
# https://github.com/SatDump/SatDump/blob/master/Dockerfile
# It is changed to clone the repo and build without GUI
ARG DEBIAN_IMAGE_TAG=trixie
FROM debian:${DEBIAN_IMAGE_TAG} AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG CMAKE_BUILD_PARALLEL_LEVEL=""
ENV TZ=Etc/UTC
# SatDump's CMakeLists adds -march=native unless ENV{CI} is defined; the GH
# arm64 runners (Cobalt/Neoverse, SVE2) would then bake in instructions the
# Pi 5 (Cortex-A76, no SVE) can't run — SIGILL at startup.
ENV CI=true

WORKDIR /usr/local/src/

# Cache apt package downloads
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt -y update && \
    apt -y upgrade && \
    apt install -y git

RUN git clone --depth 1 https://github.com/SatDump/SatDump.git satdump

WORKDIR /usr/local/src/satdump

# Cache apt package downloads for builder dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt -y update && \
    apt -y upgrade && \
    xargs -a packages.builder apt install --no-install-recommends -qy && \
    apt install --no-install-recommends -qy libsqlite3-dev

RUN cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-Wno-error -Wno-unused-parameter" \
    -Wno-dev \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DBUILD_GUI=OFF \
    -DPLUGIN_ALL=ON \
    -DBUILD_TOOLS=ON && \
    cmake --build build --target package -j${CMAKE_BUILD_PARALLEL_LEVEL:-$(nproc)} && \
    mkdir -p build-res && \
    cp -r build/satdump_*.deb build-res/

ARG DEBIAN_IMAGE_TAG=trixie
FROM debian:${DEBIAN_IMAGE_TAG} AS runner

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# The .deb and package list are bind-mounted from the builder instead of
# COPYed, so they don't end up in a layer. No recommends: they pull in ~1 GB
# (WebKitGTK, GStreamer, LLVM, GDAL, ...) that the CLI pipelines don't use.
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    --mount=type=bind,from=builder,source=/usr/local/src/satdump,target=/mnt/satdump \
    apt -y update && \
    apt -y upgrade && \
    xargs -a /mnt/satdump/packages.runner apt install --no-install-recommends -qy && \
    apt install --no-install-recommends -qy libhdf5-cpp-310 libhdf5-hl-310 && \
    apt install --no-install-recommends -qy /mnt/satdump/build-res/satdump_*.deb

COPY satdump.sh /usr/bin/satdump.sh
RUN chmod +x /usr/bin/satdump.sh

# Baked-in SatDump settings (disables Kepler/TLE auto-update); satdump.sh copies
# this into $HOME/.config/satdump/settings.json at runtime.
COPY settings.json /etc/groundstation/satdump-settings.json

WORKDIR /srv

ENTRYPOINT ["/usr/bin/satdump.sh"]
