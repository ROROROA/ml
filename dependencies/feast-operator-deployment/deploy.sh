helm repo add feast-operator https://feast-helm-charts.storage.googleapis.com
helm repo update



helm install feast-operator feast-operator/feast-operator \
  --namespace feast-operator-system \
  --wait --timeout=10m
kubectl apply -f feature-store.yaml
kubectl apply -f ui-service.yaml
# kubectl apply -f storage-pv.yaml

echo ""
echo "🎉 Feast Operator 部署完成!"
echo ""
echo "📋 有用的命令:"
echo "  查看 FeatureStore 状态: kubectl get featurestore"
echo "  查看 Feast 组件: kubectl get pods -l app.kubernetes.io/managed-by=feast-operator"
echo "  查看 Feature Server 日志: kubectl logs -l app=feast-feature-server"
echo "  端口转发访问服务: kubectl port-forward svc/feast-feature-server 6566:80"
echo ""
echo "🧪 测试特征服务: ./test-features.sh"



