# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/fatfish ./cmd/fatfish

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      poppler-utils \
      tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /out/fatfish /usr/local/bin/fatfish
COPY assets/fonts/ /app/assets/fonts/
COPY configs/fatfish.example.yaml /app/configs/fatfish.example.yaml
COPY configs/personas/example.md /app/configs/personas/example.md

ENV TZ=Asia/Shanghai
ENV GOMEMLIMIT=360MiB
EXPOSE 6185

ENTRYPOINT ["fatfish"]
CMD ["serve", "-c", "/app/configs/fatfish.yaml"]
