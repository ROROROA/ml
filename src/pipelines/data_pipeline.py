"""
数据处理与特征工程的 Prefect 工作流。
本文件采用“注册表与执行器分离”的设计模式，高效且可扩展。
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
# from prefect_shell import ShellOperation
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from typing import Optional, List, Dict, Callable
from pyspark.sql import SparkSession, DataFrame

# --- 关键导入：从我们的逻辑模块导入所有需要的函数 ---
from src.etl.feature_logic import calculate_user_rolling_rating_features
from src.etl.data_loaders import load_spine, load_ratings
# --- 关键改进：直接从 Feast 定义中导入数据源，确保路径一致性 ---
from feature_repo.feature_views import user_rolling_features_source


# def get_spark_session() -> SparkSession:
#     """获取或创建一个 SparkSession。在生产环境中由 Prefect Worker 调用。"""
#     return SparkSession.builder.appName("FeatureETL-MovieLens").enableHiveSupport().getOrCreate()


def get_spark_session() -> SparkSession:
    """获取或创建一个 SparkSession。"""
    # return SparkSession.builder.appName("FeatureETL-MovieLens").enableHiveSupport().getOrCreate()
    return SparkSession.builder.appName("FeatureETL-MovieLens").remote("sc://spark-connect-external-service:15002").getOrCreate()
    # return SparkSession.builder.remote("sc://localhost:31002").getOrCreate()

# --- 1. 定义特征计算的注册表 (Feature Registry) ---
FEATURE_REGISTRY: Dict[str, Dict] = {
    "user_rolling_ratings": {
        "logic": calculate_user_rolling_rating_features,
        "inputs": ["spine", "ratings"],
        "output_table": user_rolling_features_source.path
    },
}

# --- 2. 定义数据加载的注册表 (Loader Registry) ---
SOURCE_LOADER_REGISTRY: Dict[str, Callable] = {
    "spine": load_spine,
    "ratings": load_ratings,
}

@task
def process_features_for_single_day(
    target_date_str: str = "2019-01-15",
    feature_groups_to_process: Optional[List[str]] = None,
    spark: Optional[SparkSession] = None 
):
    """
    【核心执行器 (Executor)】
    为给定的一天，调度并计算一个或多个特征组。
    """
    # import logging
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logger = logging.getLogger(__name__)
    logger = get_run_logger()

    logger.info(f"Features processing executor started for date: {target_date_str}")
    
    is_external_spark = spark is not None
    if not is_external_spark:
        spark = get_spark_session()

    try:
        # --- 关键修复：确保目标数据库存在 ---
        # 在写入任何表之前，先创建 feature_store 数据库（如果它不存在）。
        # 这使得任务更加健壮和自给自足。
        output_db = "feature_store"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {output_db}")
        logger.info(f"Database '{output_db}' is ready.")

        if not feature_groups_to_process:
            feature_groups_to_process = list(FEATURE_REGISTRY.keys())
        
        all_required_inputs = set()
        for group in feature_groups_to_process:
            if group in FEATURE_REGISTRY:
                all_required_inputs.update(FEATURE_REGISTRY[group]["inputs"])
        
        source_dataframes: Dict[str, DataFrame] = {}
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
        logger.info(f"Required source tables: {all_required_inputs}")

        for input_name in all_required_inputs:
            loader_fn = SOURCE_LOADER_REGISTRY[input_name]
            source_dataframes[input_name] = loader_fn(spark, target_date).cache()
            
        for group_name in feature_groups_to_process:
            if group_name not in FEATURE_REGISTRY: continue
            logger.info(f"Processing feature group: '{group_name}'...")
            registry_entry = FEATURE_REGISTRY[group_name]
            kwargs_for_logic = {key: source_dataframes[key] for key in registry_entry["inputs"]}
            final_features_df = registry_entry["logic"](**kwargs_for_logic)
            logger.info(f"Saving features for group '{group_name}' to {registry_entry['output_table']}")
            final_features_df.write.mode("append").partitionBy("timestamp").saveAsTable(registry_entry['output_table'])

        logger.info(f"Successfully processed all requested feature groups for {target_date_str}.")
    
    finally:
        # 只关闭由本函数内部创建的 Spark Session
        if not is_external_spark and 'spark' in locals():
            logger.info("Closing internally-managed Spark session.")
            spark.stop()


from prefect import task, get_run_logger
# from prefect_shell import ShellOperation

@task
def simple_test_task():
    logger = get_run_logger()
    logger.info("This is a simple test task. If you see this, the import is OK.")
    print("Simple test task is running!")

# @task
# def materialize_features_to_online_store(target_date: str, feast_repo_path: str = "feature_repo"):
#     print("print materialize_features_to_online_store")
#     logger = get_run_logger()
#     logger.info("starting materialization..")
    
    # # 好的实践：为提前退出的情况也添加日志
    # if not target_date:
    #     logger.info("target_date is empty, skipping materialization.")
    #     return

    # # 1. 首先定义 command 变量
    # command = f"feast -c {feast_repo_path} materialize-incremental {target_date}"
    
    # # 2. 然后再记录它
    # logger.info(f"Running Feast materialization command: {command}")
    
    # # 3. 执行命令，并实时流式传输输出
    # ShellOperation(
    #     commands=[command],
    #     stream_output=True
    # ).run()
@flow
def data_pipeline_flow(target_date: Optional[str] = None):
    target_date_str = target_date or (datetime.now() - timedelta(days=50)).strftime('%Y-%m-%d')
    
    # processed_task = process_features_for_single_day.submit(target_date_str)
    
    # 在崩溃的任务之前，先调用这个简单的测试任务
    # test_task_future = simple_test_task.submit(wait_for=[processed_task])
    simple_test_task.submit()
    
    # materialize_features_to_online_store.submit(
    #     target_date=target_date_str, 
    #     wait_for=[test_task_future] # 让它等待测试任务
    # )

@flow
def backfill_flow(start_date: str, end_date: str, feature_groups: Optional[List[str]] = None):
    date_range = pd.date_range(start=start_date, end=end_date)
    date_strings = [d.strftime('%Y-%m-%d') for d in date_range]
    from prefect.utilities.annotations import unmapped
    process_features_for_single_day.map(
        target_date_str=date_strings, 
        feature_groups_to_process=unmapped(feature_groups)
    )

