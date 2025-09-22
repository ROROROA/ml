import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, col, current_timestamp
import socket # 引入 socket 库来获取本机IP

# no need interactive-shell-integration.yaml 
# but need kubectl port-forward svc/minio 9000:9000 -n default
# but need kubectl port-forward svc/mlflow-postgres-postgresql 5432:5432 -n default
# @REM 127.0.0.1 minio.default.svc.cluster.local
# @REM 127.0.0.1 mlflow-postgres-postgresql.default.svc.cluster.local




def get_host_ip():
    """获取本机在局域网中的IP地址"""
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 连接到一个公共DNS，但这并不会真的发送数据
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        if s:
            s.close()
    return ip

def setup_spark_session_host_driver(warehouse_bucket):
    """
    配置并返回一个 SparkSession。
    这个版本配置为从 Host (本机) 作为 Driver 连接到 K8s 集群。
    """
    driver_host_ip = get_host_ip()
    print(f"检测到本机 Driver IP 地址为: {driver_host_ip}")

    # PostgreSQL Metastore 连接信息 (保持不变)
    PG_HOST = "mlflow-postgres-postgresql.default.svc.cluster.local"
    MINIO_HOST = "http://minio.default.svc.cluster.local"
    
    # MINIO_HOST = "http://localhost"    
    # PG_HOST = "localhost"
    PG_PORT = "5432"
    PG_DATABASE = "spark"
    PG_USER = "mlflow_user"
    PG_PASSWORD = "mlflow_password"

    

        # K8s 和 Jar 挂载配置
    local_jar_path = "C:\\Users\\30630\\.ivy2.5.2\\jars\\*" 
    host_jar_path_in_k8s = "/run/desktop/mnt/host/c/Users/30630/.ivy2.5.2/jars"
    mount_path_in_container = "/opt/spark/jars-pv" 
    

            # .config("spark.jars.packages", ",".join(required_packages))
    print("开始构建 Host Driver 模式的 SparkSession...")
    
    spark = (
        SparkSession.builder
        .appName("Host-Driver-MovieLens-ETL")
        .enableHiveSupport()

        # --- K8s, Classpath, Mounts, S3 配置 ---
        .master("k8s://") 
        .config("spark.kubernetes.container.image", "my-spark-jupyter:latest")
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-operator-spark")
        
        # 将所有依赖项配置在一起，用逗号分隔

        
        .config("spark.driver.extraJavaOptions", "-Dkubernetes.trust.certificates=true")
        
        .config("spark.driver.host", driver_host_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")

                # 我们仍然保留 extraClassPath，用于加载 S3 连接器等其他所有本地 JAR 包
        .config("spark.driver.extraClassPath", f"{local_jar_path}")
        
        # Executor 的配置也使用修正后的正确路径进行挂载
        .config("spark.executor.extraClassPath", f"{mount_path_in_container}/*")
        .config("spark.kubernetes.executor.volumes.hostPath.shared-jars-hostpath.options.path", host_jar_path_in_k8s)
        .config("spark.kubernetes.executor.volumes.hostPath.shared-jars-hostpath.mount.path", mount_path_in_container)

        # S3 和 Hive Metastore 配置保持不变... 
        .config("spark.hadoop.fs.s3a.endpoint", f"{MINIO_HOST}:9000")
        .config("spark.hadoop.fs.s3a.access.key", "cXFVWCBKY6xlUVjuc8Qk")
        .config("spark.hadoop.fs.s3a.secret.key", "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", PG_USER)
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", PG_PASSWORD)
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.autoCreateTables", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "false")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        .config("spark.sql.warehouse.dir", f"s3a://{warehouse_bucket}/")
        
        .getOrCreate()
    )
    print("✅ Host Driver SparkSession 构建成功！")
    return spark

    # --- 定义存储位置 ---
SOURCE_DATA_BUCKET = "movielens"        # 存放源CSV文件的Bucket
WAREHOUSE_BUCKET = "spark-warehouse"    # 存放处理后的Parquet表数据的Bucket

# 设置SparkSession，并指定数据仓库的位置
spark = setup_spark_session_host_driver(WAREHOUSE_BUCKET)
spark.sql(f"USE raw_data")
spark.sql(f"select * from raw_data.movielens_movies limit 10").count()