from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
)
from feast.types import Float32, Int64, String
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)



user_entity = Entity(name="user_id", description="The user entity for MovieLens")
movie_entity = Entity(name="movie_id", description="The movie entity for MovieLens")

# --- 2. 定义特征视图 ---

# 特征视图 A: 用户的滚动聚合特征
# 这个视图的数据源是由我们的 data_pipeline.py 生成的历史快照表。

# user_rolling_features_source = FileSource(
#     path="feature_store.user_rolling_ratings_historical", # 这是由 data_pipeline.py 生成的表
#     timestamp_field="timestamp",
#     description="A table containing historically accurate, point-in-time correct user features.",
# )

user_rolling_features_source = SparkSource(
    table="feature_store.user_rolling_ratings_historical",
    timestamp_field="timestamp",
    # 如果你的表有创建时间戳，可以加上这个参数
    # created_timestamp_column="created",
)

# user_rolling_features_source = FileSource(
#     file_format=ParquetFormat(),
#     path="s3://spark-warehouse/feature_store.db/user_rolling_ratings_historical"
#     timestamp_field="timestamp"
# )


# user_rolling_features_source = FileSource(
#     path="feature_store.user_rolling_ratings_historical", # 这是由 data_pipeline.py 生成的表
#     timestamp_field="timestamp",
#     description="A table containing historically accurate, point-in-time correct user features.",
# )

user_rolling_features_view = FeatureView(
    name="user_rolling_features",
    entities=[user_entity],
    ttl=None,
    schema=[
        Field(name="avg_rating_past_30d", dtype=Float32),
        Field(name="rating_count_past_30d", dtype=Int64),
    ],
    source=user_rolling_features_source,
    online=True,
    tags={"group": "user_rolling_ratings"},
)


# 特征视图 B: 电影的静态特征
# 这个视图的数据源直接来自原始的 movies 表，因为电影的类型通常不会改变。
# movie_static_features_source = FileSource(
#     path="raw_data.movielens_movies", # 直接指向原始的 movies 表
#     timestamp_field="created_timestamp", # 假设有一个电影条目创建时间戳
#     description="A table containing static movie metadata.",
# )

# movie_static_features_view = FeatureView(
#     name="movie_static_features",
#     entities=[movie_entity],
#     ttl=None,
#     schema=[
#         Field(name="title", dtype=String),
#         Field(name="genres", dtype=String), # 电影类型，格式如 "Action|Adventure|Comedy"
#     ],
#     source=movie_static_features_source,
#     online=True,
#     tags={"group": "movie_static"},
# )

