#!/bin/bash
# ==============================================================================
# 本地自动化部署脚本 (版本二：Prefect 驱动)
#
# 功能:
#   极简的启动器，只负责配置环境并调用 `prefect deploy`。
#   所有复杂的构建、推送、部署逻辑都由 prefect.yaml 声明式地定义。
# ==============================================================================

# --- 配置区 ---
# 设置 Prefect 连接配置（根据部署偏好，优先确保核心组件配置正确）
if [ -z "$PREFECT_API_URL" ]; then
    echo "🔧 Setting default PREFECT_API_URL for local development..."
    export PREFECT_API_URL="http://localhost:30200/api"
fi

# 自托管模式不需要 API_KEY，设置为空
if [ -z "$PREFECT_API_KEY" ]; then
    echo "🔧 Setting PREFECT_API_KEY for self-hosted mode..."
    export PREFECT_API_KEY=""  # 自托管模式留空
fi

echo "✅ Prefect configuration:"
echo "   API_URL: $PREFECT_API_URL"
echo "   API_KEY: [自托管模式，无需密钥]"

# ----------------------------------------------------
set -e

echo "🚀 Starting deployment process (Prefect driven)..."
echo "📍 Current directory: $(pwd)"
echo "📁 Project root: $(dirname "$(realpath "$0")")/.."

# 切换到项目根目录
cd "$(dirname "$0")/.."
echo "✅ Changed to project root: $(pwd)"

# 验证 prefect.yaml 存在
if [ ! -f "prefect.yaml" ]; then
    echo "❌ Error: prefect.yaml not found in project root directory."
    echo "📍 Current directory: $(pwd)"
    echo "📋 Files in current directory:"
    ls -la
    exit 1
fi
echo "✅ Found prefect.yaml in project root"

# 步骤 1: 配置 Prefect CLI
echo "🔧 Step 1: Configuring Prefect CLI..."
prefect config set PREFECT_API_URL="$PREFECT_API_URL"
prefect config set PREFECT_API_KEY="$PREFECT_API_KEY"
echo "✅ Prefect CLI configured."
echo

#pip install docker build ..

# 步骤 2: 调用 Prefect Deploy
echo "✈️ Step 2: Handing over to 'prefect deploy' to build and deploy..."
# prefect deploy 会读取 prefect.yaml，并执行里面定义的 build 和 deploy 步骤
prefect deploy
echo "✅ All Prefect flows built and deployed successfully by Prefect."
echo

echo "🎉 Deployment process completed successfully!"