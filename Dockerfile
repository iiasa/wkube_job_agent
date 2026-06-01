FROM golang:1.24.1 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -o wagt ./cmd/main.go

FROM alpine:3.19

# Install dependencies needed during build time
RUN apk add --no-cache curl tar gzip patchelf

COPY --from=builder /app/wagt /agent/wagt
COPY ./bin/ssh /agent/ssh

# Download, extract, and bootstrap Astral standalone Python + hf_xet at docker build-time
RUN mkdir -p /agent/xet_python && \
    curl -sSL https://github.com/astral-sh/python-build-standalone/releases/download/20240107/cpython-3.10.13+20240107-x86_64-unknown-linux-gnu-install_only.tar.gz | tar -xz -C /agent/xet_python --strip-components=1 && \
    /agent/xet_python/bin/python3 -m pip install --upgrade pip && \
    /agent/xet_python/bin/python3 -m pip install hf_xet

RUN chmod -R +x /agent/wagt /agent/ssh /agent/xet_python/bin/

# Recursively copy all bundled agent executables and python virtual environment to the shared volume
ENTRYPOINT ["sh", "-c", "cp -rp /agent/* /mnt/agent/"]