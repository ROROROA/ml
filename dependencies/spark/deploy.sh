helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

helm install my-spark-operator spark-operator/spark-operator

# kubectl apply -f spark-pi.yaml

# download to c/Users/30630/.ivy2.5.2/jars
#     org.apache.hadoop_hadoop-aws-3.4.1.jar
#     org.wildfly.openssl_wildfly-openssl-1.1.3.Final.jar
#     software.amazon.awssdk_bundle-2.24.6.jar
#     iceberg-spark-runtime-4.0_2.13-1.10.0.jar

option 1
kubectl apply -f interactive-shell-integration.yaml

k8s://https://kubernetes.default.svc.cluster.local:443
localhost:30088 jupyter
localhost:30040 driver UI
1.1 process_data_in_cluster.py (client mode, driver in cluster)
1.2 process_data_local.py (client mode, driver in host)

option 2

kubectl apply -f spark-connect-nodeport-service.yaml
#  ide run notebook .   sparksession get : localhost:31002
#   UI : localhost: 30041
