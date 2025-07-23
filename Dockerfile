FROM golang:1.24.1 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -o wagt ./cmd/main.go

FROM alpine

COPY --from=builder /app/wagt /agent/wagt
COPY ./bin/ssh /agent/ssh

RUN chmod +x /agent/wagt /agent/ssh

ENTRYPOINT ["sh", "-c", "cp /agent/* /mnt/agent/"]