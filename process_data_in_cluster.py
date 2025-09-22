import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, col, current_timestamp

# work with interactive-shell-integration.yaml no need port forward

def setup_spark_session(warehouse_bucket):
    """
    配置并返回一个连接到 K8s 集群、S3 和 PostgreSQL Hive Metastore 的 SparkSession。
    这个函数接收一个参数来定义数据仓库的位置。
    """
    # K8s 和 Jar 挂载配置 (与之前调试时保持一致)
    host_jar_path_in_k8s = "/run/desktop/mnt/host/c/Users/30630/.ivy2.5.2/jars"
    mount_path_in_container = "/opt/spark/jars-pv"
    
    # PostgreSQL Metastore 连接信息
    PG_HOST = "mlflow-postgres-postgresql.default.svc.cluster.local"
    PG_PORT = "5432"
    PG_DATABASE = "spark"
    PG_USER = "mlflow_user"
    PG_PASSWORD = "mlflow_password"
    
    print("开始构建生产环境 SparkSession...")
    
    spark = (
        SparkSession.builder
        .appName("S3-To-Hive-MovieLens-ETL")
        .enableHiveSupport()

        # --- K8s, Classpath, Mounts, S3 配置 ---
        .master("k8s://https://kubernetes.default.svc.cluster.local:443")
        .config("spark.kubernetes.container.image", "my-spark-jupyter:latest")
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-operator-spark")
        .config("spark.driver.extraClassPath", f"{mount_path_in_container}/*")
        .config("spark.executor.extraClassPath", f"{mount_path_in_container}/*")
        .config("spark.kubernetes.executor.volumes.hostPath.shared-jars-hostpath.options.path", host_jar_path_in_k8s)
        .config("spark.kubernetes.executor.volumes.hostPath.shared-jars-hostpath.mount.path", mount_path_in_container)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.default.svc.cluster.local:9000")
        .config("spark.hadoop.fs.s3a.access.key", "cXFVWCBKY6xlUVjuc8Qk")
        .config("spark.hadoop.fs.s3a.secret.key", "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # --- Hive Metastore 配置 ---
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", PG_USER)
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", PG_PASSWORD)
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.autoCreateTables", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "false")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        
        # --- 数据仓库位置 ---
        # Hive表的数据将默认存储在这个 S3 Bucket 的根目录下
        .config("spark.sql.warehouse.dir", f"s3a://{warehouse_bucket}/")
        
        .getOrCreate()
    )
    print("✅ SparkSession 构建成功！")
    return spark

def manage_hive_schema(spark, db_name, tables_to_create):
    """
    管理 Hive 数据库和表，确保一个干净的运行环境。
    1. 创建数据库（如果不存在）。
    2. 删除所有目标表（如果已存在）。
    3. 重新创建所有目标表。
    """
    print(f"\n--- 步骤 1: 准备数据库和表 Schema ---")
    
    # 创建数据库
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"数据库 '{db_name}' 已存在或创建成功。")
    
    # 切换到该数据库
    spark.sql(f"USE {db_name}")
    print(f"已切换到数据库 '{db_name}'。")

    for table_name, ddl in tables_to_create.items():
        # 先删除已存在的表，确保幂等性
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"旧表 '{table_name}' (如果存在) 已被删除。")
        
        # 创建新表
        spark.sql(ddl)
        print(f"新表 '{table_name}' 已根据 DDL 创建成功。")

def main():
    """
    主ETL流程: 从S3读取数据，转换后写入Hive Metastore管理的表中。
    """
    # --- 定义存储位置 ---
    SOURCE_DATA_BUCKET = "movielens"        # 存放源CSV文件的Bucket
    WAREHOUSE_BUCKET = "spark-warehouse"    # 存放处理后的Parquet表数据的Bucket
    
    # 设置SparkSession，并指定数据仓库的位置
    spark = setup_spark_session(WAREHOUSE_BUCKET)
    
    # --- 数据库和表定义 ---
    DATABASE_NAME = "raw_data"
    RATINGS_TABLE_NAME = f"{DATABASE_NAME}.movielens_ratings"
    MOVIES_TABLE_NAME = f"{DATABASE_NAME}.movielens_movies"

    # 定义所有需要创建的表的DDL（数据定义语言）
    tables_ddl = {
        RATINGS_TABLE_NAME: f"""
            CREATE TABLE {RATINGS_TABLE_NAME} (
                userId INT,
                movieId INT,
                rating FLOAT,
                timestamp LONG,
                rating_date DATE
            )
            USING PARQUET
            PARTITIONED BY (rating_date)
        """,
        MOVIES_TABLE_NAME: f"""
            CREATE TABLE {MOVIES_TABLE_NAME} (
                movieId INT,
                title STRING,
                genres STRING,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
        """
    }

    # 执行 Schema 管理
    manage_hive_schema(spark, DATABASE_NAME, tables_ddl)

    # --- 步骤 2: 处理 ratings.csv ---
    print(f"\n--- 步骤 2: 处理 ratings 数据 ---")
    # 从源数据Bucket读取CSV
    ratings_s3_path = f"s3a://{SOURCE_DATA_BUCKET}/ml-25m/ratings.csv"
    print(f"从 S3 读取 ratings 数据: {ratings_s3_path}")

    ratings_df = spark.read.csv(ratings_s3_path, header=True, inferSchema=True).limit(100000)

    ratings_transformed_df = ratings_df \
        .withColumn("rating_timestamp", from_unixtime(col("timestamp"))) \
        .withColumn("rating_date", to_date(col("rating_timestamp"))) \
        .select("userId", "movieId", "rating", "timestamp", "rating_date")
    
    print(f"转换了 {ratings_transformed_df.count()} 条 ratings 记录。")

    print(f"开始将 ratings 数据写入到表: {RATINGS_TABLE_NAME}")
    # Spark会自动根据warehouse.dir配置将数据写入到 WAREHOUSE_BUCKET
    ratings_transformed_df.write \
        .mode("overwrite") \
        .insertInto(RATINGS_TABLE_NAME)
    print(f"✅ ratings 数据成功写入！")


    # --- 步骤 3: 处理 movies.csv ---
    print(f"\n--- 步骤 3: 处理 movies 数据 ---")
    # 从源数据Bucket读取CSV
    movies_s3_path = f"s3a://{SOURCE_DATA_BUCKET}/ml-25m/movies.csv"
    print(f"从 S3 读取 movies 数据: {movies_s3_path}")
    
    movies_df = spark.read.csv(movies_s3_path, header=True, inferSchema=True)

    movies_transformed_df = movies_df \
        .withColumn("created_timestamp", current_timestamp()) \
        .select("movieId", "title", "genres", "created_timestamp")

    print(f"转换了 {movies_transformed_df.count()} 条 movies 记录。")
    
    print(f"开始将 movies 数据写入到表: {MOVIES_TABLE_NAME}")
    # Spark会自动根据warehouse.dir配置将数据写入到 WAREHOUSE_BUCKET
    movies_transformed_df.write \
        .mode("overwrite") \
        .insertInto(MOVIES_TABLE_NAME)
    print(f"✅ movies 数据成功写入！")


    print("\n🎉 全部数据处理和加载任务成功完成！")
    spark.stop()

if __name__ == "__main__":
    main()

