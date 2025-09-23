# # test_online.py
# from feast import FeatureStore
# import pandas as pd

# # 实例化 FeatureStore，Feast 会加载 feature_store.yaml 配置
# store = FeatureStore(repo_path=".")

# # 定义一个实体 DataFrame，用于指定要获取哪个实体的特征
# # 注意：你需要使用一个你已经物化过的实体ID
# entity_rows = [
#     {"user_id": 33019}
# ]

# # 获取在线特征
# online_features = store.get_online_features(
#     entity_rows=entity_rows,
#     features=[
#         "user_rolling_features:avg_rating_past_30d",
#         "user_rolling_features:rating_count_past_30d",
#     ],
# )

# # 打印结果
# online_features_dict = online_features.to_dict()
# print(online_features_dict)