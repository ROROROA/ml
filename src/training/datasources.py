"""
本模块为 MovieLens 数据集声明式地定义训练数据源 (Training Data Sources)。
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

class TrainingDataSource:
    """一个用于定义和加载训练“骨架”(Spine)的基类。"""
    def __init__(self, name: str, table_name: str, entity_col: str, timestamp_col: str):
        self.name = name
        self.table_name = table_name
        self.entity_col = entity_col
        self.timestamp_col = timestamp_col

    def get_entity_df(
        self,
        spark: SparkSession,
        start_date: str,
        end_date: str,
        sampling_ratio: float = 1.0
    ) -> DataFrame:
        """
        从源表加载数据，并进行采样和基本的列重命名。
        子类应该重写这个方法来加入标签生成等逻辑。
        """
        query = f"""
            SELECT
                {self.entity_col},
                {self.timestamp_col},
                *
            FROM {self.table_name}
            WHERE to_date(from_unixtime({self.timestamp_col})) BETWEEN '{start_date}' AND '{end_date}'
        """
        df = spark.sql(query)
        if sampling_ratio < 1.0:
            df = df.sample(withReplacement=False, fraction=sampling_ratio)
        return df

class MovieLensRatingSource(TrainingDataSource):
    """专门为 MovieLens ratings 数据源定制的类。"""
    def get_entity_df(
        self,
        spark: SparkSession,
        start_date: str,
        end_date: str,
        sampling_ratio: float = 1.0
    ) -> DataFrame:
        # 首先调用父类的方法加载基础数据
        base_df = super().get_entity_df(spark, start_date, end_date, sampling_ratio)
        
        # --- MovieLens 特有的逻辑：生成标签 ---
        # 我们定义评分 > 3.5 为正样本 (is_liked = 1)
        final_df = base_df.withColumn(
            "is_liked",
            when(col("rating") > 3.5, 1).otherwise(0)
        ).select(
            col(self.entity_col).alias("user_id"),
            col(self.timestamp_col).alias("timestamp"),
            "is_liked",
            col("movieId").alias("movie_id") # 将 movieId 也包含进来
        )
        return final_df

# --- 在这里注册我们所有可用的训练数据源 ---
movielens_ratings_source = MovieLensRatingSource(
    name="movielens_ratings",
    table_name="raw_data.movielens_ratings",
    entity_col="userId",
    timestamp_col="timestamp"
)

# --- 创建一个注册表，以便在 Prefect Flow 中通过名字引用 ---
DATASOURCE_REGISTRY = {
    "movielens_ratings": movielens_ratings_source,
}

