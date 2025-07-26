# This file is essentially a copy of the Dockerfile from the SatDump repository
# https://github.com/SatDump/SatDump/blob/master/Dockerfile
# It is changed to clone the repo and build without GUI
ARG DEBIAN_IMAGE_TAG=bookworm
FROM debian:${DEBIAN_IMAGE_TAG} AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG CMAKE_BUILD_PARALLEL_LEVEL
ENV TZ=Etc/UTC

WORKDIR /usr/local/src/

# Cache apt package downloads
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt -y update && \
    apt -y upgrade && \
    apt install -y git

RUN git clone --depth 1 \
    --branch "$(git ls-remote --tags --sort='v:refname' https://github.com/SatDump/SatDump.git | tail -n1 | sed 's/.*\///')" \
    https://github.com/SatDump/SatDump.git satdump

WORKDIR /usr/local/src/satdump

# Cache apt package downloads for builder dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt -y update && \
    apt -y upgrade && \
    xargs -a packages.builder apt install --no-install-recommends -qy

# Cache cmake build files
RUN --mount=type=cache,target=/usr/local/src/satdump/build \
    cmake -B build \
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

ARG DEBIAN_IMAGE_TAG=bookworm
FROM debian:${DEBIAN_IMAGE_TAG} AS runner

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

COPY --from=builder /usr/local/src/satdump/packages.runner /usr/local/src/

COPY --from=builder /usr/local/src/satdump/build-res/satdump_*.deb /usr/local/src/

# Cache apt package downloads for runner dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt -y update && \
    apt -y upgrade && \
    xargs -a /usr/local/src/packages.runner apt install -qy && \
    apt install -qy /usr/local/src/satdump_*.deb && \
    rm -rf /usr/local/src/satdump

COPY satdump.sh /usr/bin/satdump.sh
RUN chmod +x /usr/bin/satdump.sh

WORKDIR /srv

ENTRYPOINT ["/usr/bin/satdump.sh"]
