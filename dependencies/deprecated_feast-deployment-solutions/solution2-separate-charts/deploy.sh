#!/bin/bash

# Feast 0.46 部署脚本 - 方案2: 独立Charts
# 分别部署feast基础组件和feature-server

echo "=== Feast 0.46 部署方案2: 独立Charts ==="
echo ""

# 1. 添加Helm仓库
echo "步骤1: 添加Feast Helm仓库..."
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update

# 2. 部署Feast基础组件（Redis等，不包含feature-server）
echo "步骤2: 部署Feast基础组件（Redis）..."
helm install feast-base feast-charts/feast --version 0.46.0 -f feast-base-values.yaml

echo "等待Redis启动..."
sleep 30

# 3. 独立部署Feature Server
echo "步骤3: 独立部署Feature Server..."
helm install feast-feature-server feast-charts/feast-feature-server --version 0.46.0 -f feature-server-values.yaml

echo "等待Feature Server启动..."
sleep 30

echo "步骤3.1: 修复Feature Server环境变量..."
kubectl patch deployment feast-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feast-feature-server","env":[{"name":"FEATURE_STORE_YAML_BASE64","value":"cHJvamVjdDogZmVhc3Rfc3RvcmUKcmVnaXN0cnk6IC90bXAvZmVhc3QvcmVnaXN0cnkuZGIKcHJvdmlkZXI6IGxvY2FsCm9ubGluZV9zdG9yZToKICB0eXBlOiByZWRpcwogIGNvbm5lY3Rpb25fc3RyaW5nOiAiZmVhc3QtYmFzZS1yZWRpcy1tYXN0ZXI6NjM3OSIKb2ZmbGluZV9zdG9yZToKICB0eXBlOiBmaWxlCmVudGl0eV9rZXlfc2VyaWFsaXphdGlvbl92ZXJzaW9uOiAyCg=="}]}]}}}}'

echo "等待修复后的Feature Server启动..."
sleep 30

# 4. 验证部署
echo "步骤4: 验证部署状态..."
echo ""
echo "=== Pod状态 ==="
kubectl get pods | grep feast

echo ""
echo "=== 服务状态 ==="
kubectl get services | grep feast

echo ""
echo "=== 部署完成 ==="
echo "Feature Server访问: kubectl port-forward service/feast-feature-server-feast-feature-server 6566:6566"
echo "Redis访问: kubectl port-forward service/feast-base-redis-master 6379:6379"
echo ""
echo "测试Feature Server: curl http://localhost:6566/health"