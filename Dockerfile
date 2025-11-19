# syntax=docker/dockerfile:1

# Build stage
FROM rust:1.91.1-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libclang-dev \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire helios source
COPY helios/ .

# Build the CLI binary
RUN cargo build --release --bin helios

# Runtime stage: minimal image with just the binary
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1000 helios && \
    mkdir -p /home/helios/.helios && \
    chown -R helios:helios /home/helios

WORKDIR /home/helios

# Copy the binary from builder
COPY --from=builder /app/target/release/helios /usr/local/bin/helios

# Switch to non-root user
USER helios

# Expose the default RPC port
EXPOSE 8545

ENTRYPOINT ["helios"]
CMD ["--help"]

