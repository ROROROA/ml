"""
本模块为 MovieLens 数据集定义了所有可复用的特征计算逻辑。
"""
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, count, unix_timestamp
import logging

def calculate_user_rolling_rating_features(
    spine: DataFrame, 
    ratings: DataFrame
) -> DataFrame:
    """
    计算用户的滚动评分特征，例如过去30天的平均评分和评分次数。
    """
    logger = logging.getLogger(__name__)
    logger.info("Applying user rolling rating feature logic for MovieLens...")

    # --- 1. 准备用于窗口函数的数据 ---
    # 为了避免 join 后的列名冲突，我们先给 ratings 表的列起一个别名
    ratings_aliased = ratings.alias("r") \
        .select(
            col("r.user_id").alias("r_user_id"),
            col("r.rating").alias("r_rating"),
            # rating_timestamp 已经是 long 类型，无需转换，但为确保类型一致，显式转换一下
            col("r.rating_timestamp").cast("long").alias("r_rating_timestamp_long")
        )

    # --- 关键修复：使用 unix_timestamp() 函数将 DATE 转换为 LONG ---
    # 这解决了之前由于数据类型不匹配导致的错误。
    spine_with_long_ts = spine.withColumn(
        "timestamp_long", unix_timestamp(col("timestamp"))
    )

    # 将“骨架”与评分数据连接
    joined_df_raw = spine_with_long_ts.join(
            ratings_aliased,
            (spine_with_long_ts.user_id == ratings_aliased.r_user_id) & (col("timestamp_long") >= ratings_aliased.r_rating_timestamp_long),
            "left"
        )
    
    # 显式选择我们需要的列，构建一个干净的、无歧义的 DataFrame
    clean_joined_df = joined_df_raw.select(
        col("user_id"),
        col("timestamp"),
        col("timestamp_long"),
        col("r_rating").alias("rating") # 将评分列重命名回 'rating'
    )
    
    # --- 关键调试步骤：打印 Schema 和样本数据 ---
    print("\n--- [DEBUG] Schema before applying window function ---")
    clean_joined_df.printSchema()
    print("\n--- [DEBUG] Sample data before applying window function (first 5 rows) ---")
    clean_joined_df.show(5, truncate=False)
    
    # --- 2. 定义窗口 ---
    days = lambda i: i * 86400 # 转换为秒
    window_spec_30d = (
        Window.partitionBy("user_id")
        .orderBy(col("timestamp_long"))
        .rangeBetween(-days(30), 0)
    )

    # --- 3. 计算特征 ---
    features_df = clean_joined_df.withColumn(
        "avg_rating_past_30d", avg("rating").over(window_spec_30d)
    ).withColumn(
        "rating_count_past_30d", count("rating").over(window_spec_30d)
    )
    
    # --- 4. 返回干净的结果 ---
    result_df = (
        features_df.select("user_id", "timestamp", "avg_rating_past_30d", "rating_count_past_30d")
        .distinct()
    )
    
    return result_df

