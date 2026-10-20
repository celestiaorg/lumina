# Private devnet runtime. Build from the repository root.

# Release versions
ARG CELESTIA_APP_VERSION=v10.4.0
ARG CELESTIA_NODE_VERSION=v0.34.3

# Celestia binary sources
FROM ghcr.io/celestiaorg/celestia-app:${CELESTIA_APP_VERSION} AS app-binary
FROM ghcr.io/celestiaorg/celestia-node:${CELESTIA_NODE_VERSION} AS node-binary

# Runtime image (Linux AMD64)
# Mainnet celestia-app requires glibc >= 2.38; Ubuntu 24.04 provides 2.39.
FROM ubuntu:24.04 AS devnet

ARG CELESTIA_APP_VERSION
ARG TARGETARCH

RUN test "$TARGETARCH" = amd64 || \
    { echo 'The devnet requires --platform linux/amd64.' >&2; exit 1; }

# Runtime dependencies and Celestia binaries
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=app-binary /bin/celestia-appd /usr/local/bin/celestia-appd
COPY --from=node-binary /bin/celestia /usr/local/bin/celestia

# Devnet tool versions and checksums
ARG DASEL_VERSION=v2.8.1
ARG DASEL_SHA256=21fda0a4dc3c779c42737eca4b37e4f187d7ab91ba6301eed97b801af84a9ea2
ARG FIBRE_SHA256=0e3332bf8b53d42c0249fb1b06ae56e3751162cb23bb9fedf915133b3da030b1

# Configuration editor: the launcher uses dasel's v2 CLI.
RUN curl --fail --location --retry 5 \
      "https://github.com/TomWright/dasel/releases/download/${DASEL_VERSION}/dasel_linux_amd64" \
      --output /usr/local/bin/dasel \
    && echo "$DASEL_SHA256  /usr/local/bin/dasel" | sha256sum -c - \
    && chmod +x /usr/local/bin/dasel

# Fibre server: use the same release as celestia-app.
RUN curl --fail --location --retry 5 \
      "https://github.com/celestiaorg/celestia-app/releases/download/${CELESTIA_APP_VERSION}/fibre_Linux_x86_64.tar.gz" \
      --output /tmp/fibre.tar.gz \
    && echo "$FIBRE_SHA256  /tmp/fibre.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/fibre.tar.gz -C /usr/local/bin fibre \
    && rm /tmp/fibre.tar.gz

# Launcher and disposable local accounts
WORKDIR /src
COPY .oss-scanner/devnet.sh .oss-scanner/devnet.sh
COPY ci/credentials ci/credentials

# Start the private devnet.
CMD ["bash", ".oss-scanner/devnet.sh"]
