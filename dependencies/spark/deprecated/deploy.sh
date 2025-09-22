kubectl apply -f spark-pvc.yaml
kubectl apply -f spark-pv.yaml
helm install my-spark-cluster bitnami/spark  -f spark-values.yaml


kubectl scale statefulset my-spark-cluster-worker --replicas=1 

kubectl config set-context --current --namespace=spark-local
kubectl create configmap spark-s3-config --from-file=spark-defaults.conf

helm upgrade my-spark-cluster bitnami/spark -f spark-s3-values.yaml 


export port
$ kubectl patch svc my-spark-cluster-master-svc  -p '{"spec":{"type":"NodePort"}}'
$ kubectl get svc 


