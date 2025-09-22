#!/bin/bash

# Feast 方案2 连通性测试脚本

echo "=== Feast 方案2 连通性测试 ==="
echo ""

# 1. 检查所有pods状态
echo "1. 检查Pod状态:"
kubectl get pods | grep feast
echo ""

# 2. 检查服务状态  
echo "2. 检查服务状态:"
kubectl get services | grep feast
echo ""

# 3. 启动端口转发
echo "3. 启动端口转发..."
kubectl port-forward service/feast-feature-server 6566:6566 &
FEATURE_SERVER_PID=$!
kubectl port-forward service/feast-base-redis-master 6379:6379 &
REDIS_PID=$!

echo "等待端口转发建立..."
sleep 5

# 4. 测试Feature Server健康检查
echo "4. 测试Feature Server健康检查:"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:6566/health | grep -q "200"; then
    echo "✅ Feature Server健康检查通过 (HTTP 200)"
else
    echo "❌ Feature Server健康检查失败"
fi

# 5. 测试Feature Server API
echo ""
echo "5. 测试Feature Server API端点:"
echo "GET /health:"
curl -s http://localhost:6566/health
echo ""

# 6. 测试Redis连接（如果有redis-cli）
echo ""
echo "6. Redis连接测试:"
if command -v redis-cli &> /dev/null; then
    if redis-cli -h localhost -p 6379 ping | grep -q "PONG"; then
        echo "✅ Redis连接正常"
    else
        echo "❌ Redis连接失败"
    fi
else
    echo "ℹ️  redis-cli未安装，跳过Redis连接测试"
    echo "   可以通过以下方式测试Redis:"
    echo "   kubectl exec -it feast-base-redis-master-0 -- redis-cli ping"
fi

echo ""
echo "=== 测试完成 ==="
echo ""
echo "清理端口转发进程:"
echo "kill $FEATURE_SERVER_PID $REDIS_PID"

# 清理
kill $FEATURE_SERVER_PID $REDIS_PID 2>/dev/null