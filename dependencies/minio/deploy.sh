helm repo add bitnami https://charts.bitnami.com/bitnami
helm install minio bitnami/minio

kubectl apply -f minio-nodeport-service.yaml 

$ kubectl get secret minio -n default -o jsonpath="{.data.rootUser}" | base64 --decode
cXFVWCBKY6xlUVjuc8Qk
$ kubectl get secret minio -n default -o jsonpath="{.data.rootPassword}" | base64 --decode
Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY


http://minio.default.svc.cluster.local:9000
localhost:9001 UI