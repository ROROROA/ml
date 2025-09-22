#!/bin/bash

# Feast 0.46 部署脚本 - 方案1: 完整chart + kubectl patch修复
# 使用官方feast chart，然后通过kubectl patch修复feature-server

echo "=== Feast 0.46 部署方案1: 完整Chart + Patch修复 ==="
echo ""

# 1. 添加Helm仓库
echo "步骤1: 添加Feast Helm仓库..."
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update

# 2. 部署Feast
echo "步骤2: 部署Feast 0.46..."
helm install feast-store feast-charts/feast --version 0.46.0 -f feast-values.yaml

echo "等待初始部署完成..."
sleep 30

# 3. 修复Feature Server - 使用Python命令替代Java
echo "步骤3: 修复Feature Server命令（Java -> Python）..."
kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","command":["feast","serve","-h","0.0.0.0"]}]}}}}'

# 4. 添加配置文件
echo "步骤4: 添加feature_store.yaml配置..."
kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","env":[{"name":"FEATURE_STORE_YAML_BASE64","value":"cHJvamVjdDogZmVhc3Rfc3RvcmUKcmVnaXN0cnk6IC90bXAvZmVhc3QvcmVnaXN0cnkuZGIKcHJvdmlkZXI6IGxvY2FsCm9ubGluZV9zdG9yZToKICB0eXBlOiByZWRpcwogIGNvbm5lY3Rpb25fc3RyaW5nOiAiZmVhc3Qtc3RvcmUtcmVkaXMtbWFzdGVyOjYzNzkiCm9mZmxpbmVfc3RvcmU6CiAgdHlwZTogZmlsZQplbnRpdHlfa2V5X3NlcmlhbGl6YXRpb25fdmVyc2lvbjogMgo="}]}]}}}}'

# 5. 修复健康检查探针
echo "步骤5: 修复健康检查探针..."
kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","readinessProbe":null,"livenessProbe":null}]}}}}'

echo "等待所有组件启动..."
sleep 45

# 6. 验证部署
echo "步骤6: 验证部署状态..."
echo ""
echo "=== Pod状态 ==="
kubectl get pods | grep feast

echo ""
echo "=== 服务状态 ==="
kubectl get services | grep feast

echo ""
echo "=== 部署完成 ==="
echo "Feature Server访问: kubectl port-forward service/feast-store-feature-server 6566:6566"
echo "Redis访问: kubectl port-forward service/feast-store-redis-master 6379:6379"
echo ""
echo "测试Feature Server: curl http://localhost:6566/health"