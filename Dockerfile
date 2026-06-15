FROM golang:1.24.1 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -o wagt ./cmd/main.go

FROM debian:bookworm-slim AS python-builder

# Install dependencies needed during download/bootstrap
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tar \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Download, extract, and bootstrap Astral standalone Python + hf_xet
RUN mkdir -p /agent/xet_python && \
    curl -sSL https://github.com/astral-sh/python-build-standalone/releases/download/20240107/cpython-3.10.13+20240107-x86_64-unknown-linux-gnu-install_only.tar.gz | tar -xz -C /agent/xet_python --strip-components=1 && \
    /agent/xet_python/bin/python3 -m pip install --upgrade pip && \
    /agent/xet_python/bin/python3 -m pip install hf_xet

FROM alpine:3.19

COPY --from=builder /app/wagt /agent/wagt
COPY ./bin/ssh /agent/ssh
COPY --from=python-builder /agent/xet_python /agent/xet_python

RUN chmod -R +x /agent/wagt /agent/ssh /agent/xet_python/bin/

# Recursively copy all bundled agent executables and python virtual environment to the shared volume
ENTRYPOINT ["sh", "-c", "cp -rp /agent/* /mnt/agent/"]