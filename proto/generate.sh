#!/bin/bash
set -euo pipefail

echo "🔧 生成 kronos-scheduler Protobuf 文件..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -z "${GOPATH:-}" ]; then
    GOPATH=$(go env GOPATH)
fi

if ! command -v protoc &>/dev/null; then
    echo "❌ protoc 未安装"
    exit 1
fi

GO_MODULE="github.com/kamalyes/kronos-scheduler"

PROTOC_PATH=$(which protoc)
PROTOC_DIR=$(dirname "$PROTOC_PATH")
PROTOC_ROOT=$(dirname "$PROTOC_DIR")
PROTOC_INCLUDE="$PROTOC_ROOT/include"

GOOGLEAPIS_PATH=""
GRPC_GATEWAY_PATH=""

if [ -d "$GOPATH/src/github.com/googleapis" ]; then
    GOOGLEAPIS_PATH="-I $GOPATH/src/github.com/googleapis"
fi

if [ -d "$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway" ]; then
    GRPC_GATEWAY_PATH="-I $GOPATH/src/github.com/grpc-ecosystem/grpc-gateway"
fi

echo "🧹 清理旧的生成文件..."
find proto -name "*.pb.go" -delete 2>/dev/null || true
find proto -name "*_grpc.pb.go" -delete 2>/dev/null || true
find proto -name "*.gw.go" -delete 2>/dev/null || true
find proto -name "*.swagger.yaml" -delete 2>/dev/null || true
find proto -name "*.swagger.json" -delete 2>/dev/null || true

echo "🚀 生成 protobuf + gRPC 代码..."
protoc -I"$PROTOC_INCLUDE" $GOOGLEAPIS_PATH $GRPC_GATEWAY_PATH -I. \
    --go_out=. --go_opt=module=$GO_MODULE \
    --go-grpc_out=. --go-grpc_opt=module=$GO_MODULE \
    proto/scheduler.proto

if [ $? -ne 0 ]; then
    echo "❌ protobuf 生成失败"
    exit 1
fi

if grep -q "google.api.http" proto/scheduler.proto 2>/dev/null; then
    echo "🌐 生成 gRPC-Gateway 代码..."
    protoc -I"$PROTOC_INCLUDE" $GOOGLEAPIS_PATH $GRPC_GATEWAY_PATH -I. \
        --grpc-gateway_out=. --grpc-gateway_opt=module=$GO_MODULE \
        --grpc-gateway_opt=generate_unbound_methods=true \
        proto/scheduler.proto

    if [ $? -ne 0 ]; then
        echo "⚠️  gRPC-Gateway 生成失败，跳过..."
    fi
fi

if grep -q "openapiv2_swagger" proto/scheduler.proto 2>/dev/null; then
    echo "📖 生成 OpenAPI 文档..."
    protoc -I"$PROTOC_INCLUDE" $GOOGLEAPIS_PATH $GRPC_GATEWAY_PATH -I. \
        --openapiv2_out=. \
        --openapiv2_opt=logtostderr=true \
        --openapiv2_opt=json_names_for_fields=false \
        --openapiv2_opt=output_format=yaml \
        proto/scheduler.proto

    if [ $? -ne 0 ]; then
        echo "⚠️  OpenAPI 文档生成失败，跳过..."
    fi
fi

if command -v protoc-go-inject-tag &>/dev/null; then
    echo "🏷️  注入结构体标签..."
    protoc-go-inject-tag --input="proto/*.pb.go" 2>/dev/null || true
fi

echo "✅ kronos-scheduler Protobuf 文件生成完成！"
