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

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/usr/local/lib/python3.11/site-packages:/app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /app /app

RUN useradd -r -u 1000 -g 0 etl
USER 1000:0

ENTRYPOINT ["python"]
