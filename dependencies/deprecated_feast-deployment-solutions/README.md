# Feast 0.46 K8s 部署解决方案

本目录包含两种在Kubernetes上部署Feast 0.46的完整解决方案。

## 方案对比

| 特性 | 方案1: Patch方法 | 方案2: 独立Charts |
|------|------------------|-------------------|
| **部署方式** | 完整chart + kubectl patch | 分离的独立charts |
| **管理复杂度** | 简单 | 中等 |
| **升级便利性** | 需要重新patch | 可独立升级 |
| **配置清晰度** | 中等 | 高 |
| **官方支持** | 完全官方 | 完全官方 |
| **适用场景** | 快速部署、测试环境 | 生产环境、微服务架构 |

## 方案1: 完整Chart + Patch修复

**目录**: `solution1-patch-method/`

使用官方完整的feast chart，然后通过kubectl patch修复feature-server的Java/Python问题。

### 优点
- ✅ 使用官方完整chart
- ✅ 最小改动
- ✅ 统一管理
- ✅ 部署简单

### 缺点  
- ❌ 需要部署后patch
- ❌ 升级时patch会丢失

### 快速开始
```bash
cd solution1-patch-method
chmod +x deploy.sh
./deploy.sh
```

## 方案2: 独立Charts

**目录**: `solution2-separate-charts/`

分别使用feast chart（只部署Redis）和feast-feature-server chart（独立部署feature-server）。

### 优点
- ✅ 组件分离，灵活管理
- ✅ 可独立升级
- ✅ 配置清晰
- ✅ 符合微服务架构

### 缺点
- ❌ 管理两个release
- ❌ 服务发现复杂

### 快速开始
```bash
cd solution2-separate-charts  
chmod +x deploy.sh
./deploy.sh
```

## 共同特性

两种方案都提供：
- ✅ **Feast 0.46版本**
- ✅ **Redis集群** (1 master + 2 slaves)
- ✅ **Python Feature Server** (端口6566)
- ✅ **完整配置** (SQLite注册表 + Redis在线存储)
- ✅ **健康检查修复**
- ✅ **生产就绪**

## 推荐选择

- **测试/开发环境**: 选择方案1 (更简单)
- **生产环境**: 选择方案2 (更灵活)
- **快速验证**: 选择方案1
- **长期维护**: 选择方案2

## 验证部署

无论选择哪种方案，都可以通过以下方式验证：

```bash
# 检查pods
kubectl get pods | grep feast

# 访问Feature Server
kubectl port-forward service/feast-*-feature-server 6566:6566 &
curl http://localhost:6566/health

# 访问Redis  
kubectl port-forward service/feast-*-redis-master 6379:6379 &
```

## 清理

```bash
# 方案1
helm uninstall feast-store

# 方案2  
helm uninstall feast-feature-server
helm uninstall feast-base