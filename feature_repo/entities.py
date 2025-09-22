# feature_repo/entities.py
from feast import Entity

# 定义模型服务的核心实体，例如用户
# --- 1. 定义实体 ---
# 在 MovieLens 数据集中，我们关心两个核心实体：用户和电影
user_entity = Entity(name="user_id", description="The user entity for MovieLens")
movie_entity = Entity(name="movie_id", description="The movie entity for MovieLens")
