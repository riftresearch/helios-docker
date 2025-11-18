# syntax=docker/dockerfile:1

# Build stage: prepare dependency list
FROM rust:1.83-slim-bookworm AS chef
WORKDIR /app
RUN cargo install cargo-chef

# Analyze dependencies
FROM chef AS planner
COPY helios/. .
RUN cargo chef prepare --recipe-path recipe.json

# Build stage: cache dependencies
FROM chef AS builder
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this layer is cached as long as dependencies don't change
RUN cargo chef cook --release --recipe-path recipe.json

# Build the application
COPY helios/. .
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

