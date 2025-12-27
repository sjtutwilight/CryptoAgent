# syntax=docker/dockerfile:1

FROM golang:1.22 AS builder
WORKDIR /src
ENV GOPROXY=https://goproxy.cn,direct
COPY datainjector/worker/go.mod datainjector/worker/go.sum ./
RUN go mod download
COPY datainjector/worker/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/worker ./cmd/worker

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/worker /usr/local/bin/worker
COPY datainjector/worker/configs ./configs
ENV WORKER_CONFIG_PATH=/app/configs/base.yaml
ENTRYPOINT ["/usr/local/bin/worker"]
CMD ["--config", "/app/configs/base.yaml"]
