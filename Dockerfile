# Multi-stage build for Superset with DataFusion support
# Stage 1: Build DataFusion packages
FROM debian:bullseye-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    python3-dev \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.89.0
ENV PATH="/root/.cargo/bin:${PATH}"

# Install maturin
RUN pip3 install maturin

WORKDIR /app

# Copy Rust project and build wheel
COPY Cargo.toml Cargo.lock pyproject.toml ./
COPY src/ ./src/
RUN maturin build --release --out dist

# Copy Python packages and build wheels
COPY python/ ./python/
RUN pip3 install build wheel
RUN cd python/sqlalchemy_datafusion_pkg && python3 -m build --wheel --outdir /app/dist
RUN cd python/superset_datafusion && python3 -m build --wheel --outdir /app/dist

# Stage 2: Superset with DataFusion
FROM apache/superset:4.0.1

# Switch to root for installation
USER root

# Install missing dependencies
RUN pip install pymysql

# Copy and install DataFusion packages
COPY --from=builder /app/dist/*.whl /tmp/wheels/
RUN pip install --no-deps /tmp/wheels/*.whl

# Copy configuration
COPY superset_config_datafusion.py /app/superset_config_datafusion.py

# Set environment variables
ENV SUPERSET_CONFIG_PATH=/app/superset_config_datafusion.py

# Switch back to superset user
USER superset

# Expose port
EXPOSE 8088

