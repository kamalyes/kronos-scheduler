@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul

echo 🔧 生成 kronos-scheduler Protobuf 文件...

cd /d %~dp0..

if not defined GOPATH (
    set "GOPATH=%USERPROFILE%\go"
)

where protoc >nul 2>nul
if !errorlevel! neq 0 (
    echo ❌ protoc 未安装
    pause
    exit /b 1
)

set "GO_MODULE=github.com/kamalyes/kronos-scheduler"

for /f "tokens=*" %%i in ('where protoc') do set "PROTOC_PATH=%%i"
for %%i in ("%PROTOC_PATH%") do set "PROTOC_DIR=%%~dpi"
set "PROTOC_INCLUDE=%PROTOC_DIR%..\include"

set "GOOGLEAPIS_PATH="
set "GRPC_GATEWAY_PATH="

if exist "!GOPATH!\src\github.com\googleapis" (
    set "GOOGLEAPIS_PATH=-I !GOPATH!\src\github.com\googleapis"
)

if exist "!GOPATH!\src\github.com\grpc-ecosystem\grpc-gateway" (
    set "GRPC_GATEWAY_PATH=-I !GOPATH!\src\github.com\grpc-ecosystem\grpc-gateway"
)

echo 🧹 清理旧的生成文件...
del /q proto\*.pb.go 2>nul
del /q proto\*_grpc.pb.go 2>nul
del /q proto\*.gw.go 2>nul
del /q proto\*.swagger.yaml 2>nul
del /q proto\*.swagger.json 2>nul

echo 🚀 生成 protobuf + gRPC 代码...
protoc -I"!PROTOC_INCLUDE!" !GOOGLEAPIS_PATH! !GRPC_GATEWAY_PATH! -I. --go_out=. --go_opt=module=!GO_MODULE! --go-grpc_out=. --go-grpc_opt=module=!GO_MODULE! proto\scheduler.proto

if !errorlevel! neq 0 (
    echo ❌ protobuf 生成失败
    pause
    exit /b 1
)

findstr /c:"google.api.http" proto\scheduler.proto >nul 2>nul
if !errorlevel! equ 0 (
    echo 🌐 生成 gRPC-Gateway 代码...
    protoc -I"!PROTOC_INCLUDE!" !GOOGLEAPIS_PATH! !GRPC_GATEWAY_PATH! -I. --grpc-gateway_out=. --grpc-gateway_opt=module=!GO_MODULE! --grpc-gateway_opt=generate_unbound_methods=true proto\scheduler.proto

    if !errorlevel! neq 0 (
        echo ⚠️  gRPC-Gateway 生成失败，跳过...
    )
)

findstr /c:"openapiv2_swagger" proto\scheduler.proto >nul 2>nul
if !errorlevel! equ 0 (
    echo 📖 生成 OpenAPI 文档...
    protoc -I"!PROTOC_INCLUDE!" !GOOGLEAPIS_PATH! !GRPC_GATEWAY_PATH! -I. --openapiv2_out=. --openapiv2_opt=logtostderr=true --openapiv2_opt=json_names_for_fields=false --openapiv2_opt=output_format=yaml proto\scheduler.proto

    if !errorlevel! neq 0 (
        echo ⚠️  OpenAPI 文档生成失败，跳过...
    )
)

where protoc-go-inject-tag >nul 2>nul
if !errorlevel! equ 0 (
    echo 🏷️  注入结构体标签...
    protoc-go-inject-tag --input="proto\*.pb.go" 2>nul
)

echo ✅ kronos-scheduler Protobuf 文件生成完成！
pause
