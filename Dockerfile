# syntax=docker/dockerfile:1

# ---------- Stage 1: 编译 go-enry CLI（Linguist 级语言检测） ----------
FROM golang:1.23-alpine AS enry-builder
ARG ENRY_VERSION=v2.9.6
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
RUN go install github.com/go-enry/go-enry/v2/cmd/enry@${ENRY_VERSION}

# ---------- Stage 2: 运行时 ----------
FROM python:3.12-slim

# 可选功能组: 默认 dedup+parquet; 重型组 scancode / pii (torch, 数 GB) 按需构建
#   docker build --build-arg EXTRAS=dedup,parquet,scancode -t dataflow:scancode .
ARG EXTRAS="dedup,parquet"

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY --from=enry-builder /go/bin/enry /usr/local/bin/enry

WORKDIR /workspace
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install ".[${EXTRAS}]"

# dataflow 为 pyproject 定义的 console script
ENTRYPOINT ["dataflow"]
CMD ["--help"]
