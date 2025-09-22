# from feast import FeatureStore
# import pandas as pd
# from datetime import datetime, timedelta

# # 1. 创建 FeatureStore 实例
# store = FeatureStore(repo_path=".")

# # 2. 准备实体 DataFrame
# # 假设你想获取用户 ID 1, 2, 3 在某个时间点的特征
# entity_df = pd.DataFrame.from_dict(
#     {
#         "user_id": [1, 2, 3],
#         "event_timestamp": [
#             pd.to_datetime("2019-01-16T12:00:00Z", utc=True),
#             pd.to_datetime("2019-01-16T12:00:00Z", utc=True),
#             pd.to_datetime("2019-01-16T12:00:00Z", utc=True),
#         ],
#     }
# )

# # 3. 获取历史特征
# # 注意：这里会使用 Spark 作为引擎来处理数据
# training_df = store.get_historical_features(
#     entity_df=entity_df,
#     features=[
#         "user_rolling_features:avg_rating_past_30d",
#         "user_rolling_features:rating_count_past_30d",
#     ],
# ).to_df()

# # 4. 打印结果，检查是否成功获取数据
# print("Successfully retrieved historical features:")
# print(training_df)