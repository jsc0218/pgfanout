FROM postgres:17-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE NOTICE /build/
COPY src/ /build/src/
RUN pip install --no-cache-dir --break-system-packages /build \
    && rm -rf /build

# Run the tools directly, e.g. `docker run IMAGE pgfanout-dump --help`.
ENTRYPOINT []
