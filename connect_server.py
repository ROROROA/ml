# connect_server.py
import time
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    一个简单的脚本，用于启动一个长期运行的 Spark Session，作为 Spark Connect 的服务端。
    所有复杂的配置都通过 `spark-submit` 命令的 `--conf` 参数从外部传入。
    """
    print("Starting Spark Connect Server...")

    # 这个脚本只需要确保 Spark Connect 插件被启用，并支持 Hive 即可。
    spark = (
        SparkSession.builder
        # 启用 Spark Connect 插件，这是服务端运行的必要条件
        .config("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
        # 启用 Hive 支持，这样才能使用 Hive Metastore
        .enableHiveSupport()
        .getOrCreate()
    )
    
    print("✅ Spark Connect Server is running and waiting for connections.")
    print("All configurations (S3, Hive, etc.) have been applied from the spark-submit command.")
    
    # 无限循环，保持 SparkSession 存活，以便客户端随时连接
    while True:
        time.sleep(60)
