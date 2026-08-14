GO ?= go
GOFMT ?= gofmt

GO_FILES := $(shell git ls-files '*.go')

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check build test test-unit test-integration test-all race vet bench tidy check

help: ## 显示可用命令
	@awk 'BEGIN {FS = ":.*##"; printf "用法: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## 格式化 Go 源码
	$(GO) fmt ./...

fmt-check: ## 检查 Go 源码是否已格式化
	@test -z "$$($(GOFMT) -l $(GO_FILES))" || { echo "以下文件需要 gofmt:"; $(GOFMT) -l $(GO_FILES); exit 1; }

build: ## 编译所有 Go package
	$(GO) build ./...

test: test-all ## 运行全部测试

test-unit: ## 运行内部单元测试
	$(GO) test ./internal/...

test-integration: ## 运行多节点集成测试（自动启动嵌入式 Etcd）
	$(GO) test ./example

test-all: ## 运行全部测试（包含集成测试）
	$(GO) test ./...

race: ## 运行全部测试的竞态检测
	$(GO) test -race ./...

vet: ## 运行 go vet
	$(GO) vet ./...

bench: ## 运行底层缓存 benchmark
	$(GO) test ./internal/store -bench=. -benchmem

tidy: ## 整理 Go module 依赖
	$(GO) mod tidy

check: fmt-check vet test ## 执行格式、静态检查和全部测试
