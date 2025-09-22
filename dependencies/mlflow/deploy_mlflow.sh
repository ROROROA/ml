#!/bin/bash

# 添加 MLflow Helm 仓库
helm repo add mlflow https://community-charts.github.io/helm-charts/

# 更新 Helm 仓库
helm repo update


# 安装 MLflow 连接postgresql
helm install mlflow mlflow/mlflow -f mlflow-values-fixed.yaml
kubectl apply -f mlflow-nodeport-service.yaml