"""
本模块为 MovieLens 数据集定义了所有可复用的数据加载逻辑。
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from datetime import datetime, timedelta

def load_spine(spark: SparkSession, target_date: datetime) -> DataFrame:
    """
    加载“时间骨架”，即在目标日期进行了评分的所有用户。
    在 MovieLens 场景中，这等同于当天的活跃用户。
    """
    target_date_str = target_date.strftime('%Y-%m-%d')
    # 假设 ratings 表按 rating_date 分区
    return spark.sql(f"""
        SELECT DISTINCT 
            userId as user_id, 
            to_date('{target_date_str}', 'yyyy-MM-dd') as timestamp 
        FROM 
            raw_data.movielens_ratings
        WHERE 
            rating_date = to_date('{target_date_str}', 'yyyy-MM-dd')
    """)

def load_ratings(spark: SparkSession, target_date: datetime) -> DataFrame:
    """
    加载评分数据。为了优化，只加载计算所需时间窗口内的数据。
    """
    # 窗口期为30天，我们加载40天的数据以确保覆盖
    start_filter_date = (target_date - timedelta(days=40)).strftime('%Y-%m-%d')
    return spark.table("raw_data.movielens_ratings").filter(
        f"rating_date >= to_date('{start_filter_date}', 'yyyy-MM-dd')"
    ).select(
        col("userId").alias("user_id"),
        col("rating"),
        col("timestamp").alias("rating_timestamp")
    )

def load_movies(spark: SparkSession, target_date: datetime) -> DataFrame:
    """
    加载电影元数据。这是一个静态表，target_date 未被使用，但保持函数签名一致。
    """
    return spark.table("raw_data.movielens_movies").select(
        col("movieId").alias("movie_id"),
        col("title"),
        col("genres")
    )

