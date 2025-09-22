# Feast 0.46 部署方案1: 完整Chart + Patch修复

## 方案概述

使用官方完整的 `feast` chart，然后通过 `kubectl patch` 命令修复 feature-server 的 Java/Python 问题。

## 优点
- ✅ 使用官方完整chart，保持官方支持
- ✅ 最小改动，只修复必要的问题
- ✅ 所有组件统一管理
- ✅ 升级时只需要重新应用patch

## 缺点
- ❌ 需要部署后手动patch
- ❌ Helm升级时patch会丢失，需要重新应用

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

# 部署
helm install feast-store feast-charts/feast --version 0.46.0 -f feast-values.yaml

# 修复feature-server
kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","command":["feast","serve","-h","0.0.0.0"]}]}}}}'

kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","env":[{"name":"FEATURE_STORE_YAML_BASE64","value":"cHJvamVjdDogZmVhc3Rfc3RvcmUKcmVnaXN0cnk6IC90bXAvZmVhc3QvcmVnaXN0cnkuZGIKcHJvdmlkZXI6IGxvY2FsCm9ubGluZV9zdG9yZToKICB0eXBlOiByZWRpcwogIGNvbm5lY3Rpb25fc3RyaW5nOiAiZmVhc3Qtc3RvcmUtcmVkaXMtbWFzdGVyOjYzNzkiCm9mZmxpbmVfc3RvcmU6CiAgdHlwZTogZmlsZQplbnRpdHlfa2V5X3NlcmlhbGl6YXRpb25fdmVyc2lvbjogMgo="}]}]}}}}'

kubectl patch deployment feast-store-feature-server -p '{"spec":{"template":{"spec":{"containers":[{"name":"feature-server","readinessProbe":null,"livenessProbe":null}]}}}}'
```

## 验证部署

```bash
# 检查pod状态
kubectl get pods | grep feast

# 检查服务
kubectl get services | grep feast

# 访问服务
kubectl port-forward service/feast-store-feature-server 6566:6566 &
kubectl port-forward service/feast-store-redis-master 6379:6379 &

# 测试
curl http://localhost:6566/health
```

## 清理

```bash
helm uninstall feast-store
```

## 配置说明

- **Redis**: 1个master + 2个slave，无认证
- **Feature Server**: Python版本，监听6566端口
- **存储**: SQLite注册表 + Redis在线存储 + 文件离线存储