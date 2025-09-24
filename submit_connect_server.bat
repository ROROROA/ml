@echo off
rem 这个批处理脚本用于向 Kubernetes 提交一个功能完备的 Spark Connect 服务端应用

echo.
echo [INFO] 设置 Spark 环境变量...

rem 设置 SPARK_HOME，指向 pyspark 的安装位置
set "SPARK_HOME=C:\Users\30630\AppData\Roaming\Python\Python312\site-packages\pyspark"
echo [INFO] SPARK_HOME 已设置为: %SPARK_HOME%

rem --- 定义路径变量 ---
rem 这是你 Windows 主机上的 JAR 包路径，在 K8s Node 中对应的路径 (适用于 Docker Desktop)
set "HOST_JAR_PATH_IN_K8S=/run/desktop/mnt/host/c/Users/30630/.ivy2.5.2/jars"
rem 这是 JAR 包在 Driver 和 Executor Pod 内部的挂载路径
set "MOUNT_PATH_IN_CONTAINER=/opt/spark/jars-pv"

echo.
echo [INFO] 准备执行 spark-submit 命令...
echo.

rem 执行 spark-submit 命令
"%SPARK_HOME%\bin\spark-submit.cmd" ^
  --master k8s://https://kubernetes.docker.internal:6443 ^
  --deploy-mode cluster ^
  --name spark-connect-server ^
  --conf spark.kubernetes.container.image=my-spark-jupyter:latest ^
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-operator-spark ^
  --conf spark.kubernetes.namespace=default ^
  --conf spark.driver.extraJavaOptions="-Dkubernetes.trust.certificates=true" ^
  --conf spark.kubernetes.file.upload.path=s3a://spark-warehouse/spark-uploads ^
  --conf spark.driver.extraClassPath=%MOUNT_PATH_IN_CONTAINER%/* ^
  --conf spark.executor.extraClassPath=%MOUNT_PATH_IN_CONTAINER%/* ^
  --conf spark.kubernetes.driver.volumes.hostPath.shared-jars.mount.path=%MOUNT_PATH_IN_CONTAINER% ^
  --conf spark.kubernetes.driver.volumes.hostPath.shared-jars.options.path=%HOST_JAR_PATH_IN_K8S% ^
  --conf spark.kubernetes.executor.volumes.hostPath.shared-jars.mount.path=%MOUNT_PATH_IN_CONTAINER% ^
  --conf spark.kubernetes.executor.volumes.hostPath.shared-jars.options.path=%HOST_JAR_PATH_IN_K8S% ^
  --conf spark.hadoop.fs.s3a.endpoint=http://minio.default.svc.cluster.local:9000 ^
  --conf spark.hadoop.fs.s3a.access.key=cXFVWCBKY6xlUVjuc8Qk ^
  --conf spark.hadoop.fs.s3a.secret.key=Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY ^
  --conf spark.hadoop.fs.s3a.path.style.access=true ^
  --conf spark.sql.catalogImplementation=hive ^
  --conf spark.hadoop.javax.jdo.option.ConnectionDriverName=org.postgresql.Driver ^
  --conf spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:postgresql://mlflow-postgres-postgresql.default.svc.cluster.local:5432/spark ^
  --conf spark.hadoop.javax.jdo.option.ConnectionUserName=mlflow_user ^
  --conf spark.hadoop.javax.jdo.option.ConnectionPassword=mlflow_password ^
  --conf spark.hadoop.datanucleus.autoCreateSchema=true ^
  --conf spark.hadoop.datanucleus.autoCreateTables=true ^
  --conf spark.hadoop.datanucleus.fixedDatastore=false ^
  --conf spark.hadoop.hive.metastore.schema.verification=false ^
  --conf spark.sql.warehouse.dir=s3a://spark-warehouse/ ^
  --conf spark.driver.memory=600m ^
  --conf spark.executor.memory=600m ^
  connect_server.py

echo.
echo [INFO] 命令执行完毕。
pause

