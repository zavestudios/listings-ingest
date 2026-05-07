# Stage 1: Builder - Install dependencies and compile bytecode
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements-runner.txt .
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements-runner.txt

# Copy application code
COPY etl/ ./etl/

# Compile Python files to bytecode for faster startup and smaller runtime footprint
RUN python -m compileall -b /app

# Stage 2: Runtime - Minimal distroless image
FROM gcr.io/distroless/python3-debian12:latest

WORKDIR /app

# Set Python path to include installed packages
ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages:/app

# Copy Python dependencies from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application code (bytecode preferred, source as fallback)
COPY --from=builder /app /app

# Run as non-root user (numeric UID for distroless compatibility)
# UID 1000 matches the 'etl' user we previously created
USER 1000:1000

ENTRYPOINT ["python"]
