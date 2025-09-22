# Feast 0.46 快速开始指南

## 🚀 一键部署

### 方案1: Patch方法 (推荐用于测试)
```bash
cd feast-deployment-solutions/solution1-patch-method
chmod +x deploy.sh
./deploy.sh
```

### 方案2: 独立Charts (推荐用于生产)
```bash
cd feast-deployment-solutions/solution2-separate-charts  
chmod +x deploy.sh
./deploy.sh
```

## ✅ 验证部署

```bash
# 检查所有组件状态
kubectl get pods | grep feast

# 应该看到类似输出:
# feast-*-feature-server-*   1/1     Running   0          2m
# feast-*-redis-master-0     1/1     Running   0          3m  
# feast-*-redis-slave-0      1/1     Running   0          3m
# feast-*-redis-slave-1      1/1     Running   0          3m
```

## 🔗 访问服务

```bash
# Feature Server
kubectl port-forward service/feast-*-feature-server 6566:6566 &

# Redis
kubectl port-forward service/feast-*-redis-master 6379:6379 &

# 测试Feature Server
curl http://localhost:6566/health
```

## 🧹 清理

```bash
# 方案1
helm uninstall feast-store

# 方案2  
helm uninstall feast-feature-server
helm uninstall feast-base
```

## 📚 详细文档

- [总体说明](README.md)
- [方案1详细文档](solution1-patch-method/README.md)  
- [方案2详细文档](solution2-separate-charts/README.md)