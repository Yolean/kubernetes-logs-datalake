FROM debian:trixie@sha256:2c91e484d93f0830a7e05a2b9d92a7b102be7cab562198b984a84fdbc7806d91 AS build

ARG FLB_VERSION=v4.2.2

RUN set -ex; \
  export DEBIAN_FRONTEND=noninteractive; \
  buildDeps=' \
    build-essential \
    cmake \
    curl \
    ca-certificates \
    git \
    make \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    libyaml-dev \
    flex \
    bison \
    wget \
    lsb-release \
    gnupg \
  '; \
  apt-get update; \
  apt-get install -y --no-install-recommends $buildDeps; \
  rm -rf /var/lib/apt/lists/*

# Apache Arrow for Parquet support
# https://docs.fluentbit.io/manual/data-pipeline/outputs/s3#build-requirements-for-parquet
RUN set -ex; \
  export DEBIAN_FRONTEND=noninteractive; \
  wget https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-trixie.deb; \
  apt-get install -y ./apache-arrow-apt-source-latest-trixie.deb; \
  rm apache-arrow-apt-source-latest-trixie.deb; \
  apt-get update; \
  apt-get install -y --no-install-recommends libarrow-glib-dev libparquet-glib-dev; \
  rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch $FLB_VERSION https://github.com/fluent/fluent-bit.git /src/fluent-bit

WORKDIR /src/fluent-bit/build

RUN cmake \
    -DFLB_RELEASE=On \
    -DFLB_JEMALLOC=On \
    -DFLB_TLS=On \
    -DFLB_SHARED_LIB=Off \
    -DFLB_EXAMPLES=Off \
    -DFLB_HTTP_SERVER=On \
    -DFLB_IN_SYSTEMD=Off \
    -DFLB_OUT_KAFKA=Off \
    -DFLB_ARROW=On \
    -DFLB_WASM=Off \
    -DFLB_LUAJIT=Off \
    ..

RUN make -j$(nproc)

RUN set -ex; \
  /src/fluent-bit/build/bin/fluent-bit --version; \
  mkdir -p /fluent-bit/bin /fluent-bit/etc; \
  cp /src/fluent-bit/build/bin/fluent-bit /fluent-bit/bin/; \
  cp /src/fluent-bit/conf/fluent-bit.conf /fluent-bit/etc/; \
  cp /src/fluent-bit/conf/parsers.conf /fluent-bit/etc/; \
  cp /src/fluent-bit/conf/plugins.conf /fluent-bit/etc/

# Collect shared libraries needed at runtime via ldd
# Normalize /lib/ to /usr/lib/ since distroless has /lib as a symlink
RUN set -ex; \
  PKG_PREFIX="$(uname -m)-linux-gnu"; \
  LIBDIR=/runtime-libs/usr/lib/$PKG_PREFIX; \
  mkdir -p "$LIBDIR"; \
  ldd /fluent-bit/bin/fluent-bit | awk '/=>/ {print $3}' | sort -u | while read lib; do \
    [ -f "$lib" ] || continue; \
    cp -dav "$lib" "$LIBDIR"/; \
    real=$(readlink -f "$lib"); \
    [ "$real" != "$lib" ] && cp -dav "$real" "$LIBDIR"/; \
  done; \
  ls "$LIBDIR"

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:5c94e1d2e831f0fadfe4048427f6ff3a91481606da2841c5b26674220ac84d2d

COPY --from=build /fluent-bit/bin/fluent-bit /usr/local/bin/
COPY --from=build /fluent-bit/etc/ /fluent-bit/etc/
COPY --from=build /runtime-libs/ /

EXPOSE 2020

ENTRYPOINT ["/usr/local/bin/fluent-bit"]
CMD ["-c", "/fluent-bit/etc/fluent-bit.conf"]
