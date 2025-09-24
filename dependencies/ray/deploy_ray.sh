#!/bin/bash

# 添加 Ray Helm 仓库
helm repo add bitnami https://charts.bitnami.com/bitnami

# 更新 Helm 仓库
helm repo update

# 安装 KubeRay 操作符
helm install ray bitnami/kuberay \
  --set cluster.head.resources.requests.memory=512Mi \
  --set cluster.head.resources.limits.memory=2000Mi \
  --set cluster.worker.resources.requests.memory=256Mi \
  --set cluster.worker.resources.limits.memory=512Mi \
  --set cluster.worker.resources.requests.cpu=100m \
  --set cluster.worker.resources.limits.cpu=200m \
  --set cluster.worker.replicas=1 \
  --set rayImage.registry="" \
  --set rayImage.repository="my-ray" \
  --set rayImage.tag="2.49.0-debian-12-r0" \
  --set global.security.allowInsecureImages=true

echo "等待 Ray 集群启动..."
sleep 30

# 检查 Ray 集群状态
echo "检查 Ray 集群状态:"
kubectl get rayclusters
kubectl get pods -l app.kubernetes.io/instance=ray

echo ""
echo "Ray Dashboard 访问方式:"
echo "1. 运行命令: kubectl port-forward service/ray-kuberay-cluster-head-svc 8266:8265"
echo "2. 在浏览器中访问: http://localhost:8266"
echo ""
echo "Ray 客户端连接地址: ray://localhost:10001"