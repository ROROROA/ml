# Feast 0.46 部署方案2: 独立Charts

## 方案概述

分别使用两个独立的chart：
1. `feast` chart - 只部署Redis等基础组件，禁用feature-server
2. `feast-feature-server` chart - 独立部署feature-server

## 优点
- ✅ 组件分离，更灵活的管理
- ✅ 可以独立升级各个组件
- ✅ 配置更清晰，避免patch
- ✅ 更符合微服务架构

## 缺点
- ❌ 需要管理两个独立的Helm release
- ❌ 服务发现需要注意命名

## 部署步骤

### 1. 快速部署
```bash
chmod +x deploy.sh
./deploy.sh
```

### 2. 手动部署
```bash
# 添加仓库
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update

# 部署基础组件（Redis）
helm install feast-base feast-charts/feast --version 0.46.0 -f feast-base-values.yaml

# 等待Redis启动
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis --timeout=300s

# 独立部署Feature Server
helm install feast-feature-server feast-charts/feast-feature-server --version 0.46.0 -f feature-server-values.yaml
```

## 验证部署

### 快速测试
```bash
# 运行连通性测试脚本
chmod +x test-connectivity.sh
./test-connectivity.sh
```

### 手动验证
```bash
# 检查pod状态
kubectl get pods | grep feast

# 检查服务
kubectl get services | grep feast

# 访问服务
kubectl port-forward service/feast-feature-server 6566:6566 &
kubectl port-forward service/feast-base-redis-master 6379:6379 &

# 测试Feature Server
curl http://localhost:6566/health
# 应该返回: HTTP 200 OK

# 测试Redis (在pod内)
kubectl exec -it feast-base-redis-master-0 -- redis-cli ping
# 应该返回: PONG
```

## 连通性测试结果

✅ **成功的部署应该显示:**
```
feast-base-redis-master-0                   1/1     Running   0          8m
feast-base-redis-slave-0                    1/1     Running   0          8m  
feast-base-redis-slave-1                    1/1     Running   0          8m
feast-feature-server-7675dc9bbf-hth47       1/1     Running   0          2m
```

✅ **Feature Server健康检查:**
```bash
curl http://localhost:6566/health
# HTTP 200 OK - 服务正常
```

## 故障排除

### Feature Server启动失败
如果Feature Server显示 `CrashLoopBackOff`，通常是环境变量问题：

```bash
# 检查日志
kubectl logs -l app.kubernetes.io/name=feast-feature-server

# 如果看到 "Can't find feature repo configuration file"
# 手动应用patch修复:
kubectl patch deployment feast-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feast-feature-server","env":[{"name":"FEATURE_STORE_YAML_BASE64","value":"cHJvamVjdDogZmVhc3Rfc3RvcmUKcmVnaXN0cnk6IC90bXAvZmVhc3QvcmVnaXN0cnkuZGIKcHJvdmlkZXI6IGxvY2FsCm9ubGluZV9zdG9yZToKICB0eXBlOiByZWRpcwogIGNvbm5lY3Rpb25fc3RyaW5nOiAiZmVhc3QtYmFzZS1yZWRpcy1tYXN0ZXI6NjM3OSIKb2ZmbGluZV9zdG9yZToKICB0eXBlOiBmaWxlCmVudGl0eV9rZXlfc2VyaWFsaXphdGlvbl92ZXJzaW9uOiAyCg=="}]}]}}}}'
```

### Redis连接问题
```bash
# 检查Redis服务
kubectl get services | grep redis

# 测试Redis连接
kubectl exec -it feast-base-redis-master-0 -- redis-cli ping
```

## 清理

```bash
helm uninstall feast-feature-server
helm uninstall feast-base
```

## 配置说明

### 基础组件 (feast-base)
- **Redis**: 1个master + 2个slave，无认证
- **Feature Server**: 禁用
- **Transformation Service**: 禁用

### Feature Server (feast-feature-server)
- **镜像**: feastdev/feature-server:0.46.0
- **命令**: Python版本 (feast serve)
- **配置**: 通过环境变量提供feature_store.yaml
- **健康检查**: 禁用（避免grpc-health-probe问题）

## 服务命名

- Redis Master: `feast-base-redis-master`
- Redis Slave: `feast-base-redis-slave`  
- Feature Server: `feast-feature-server-feast-feature-server`