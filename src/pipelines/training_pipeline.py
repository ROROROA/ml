"""
模型训练、验证与注册的 Prefect 工作流。
本文件通过组合多个子流程（subflows）来处理混合执行环境（Spark 和 Ray）。
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
# --- 关键导入：引入 RayTaskRunner ---
from prefect_ray.task_runners import RayTaskRunner
import prefect.variables

import os
from datetime import datetime
from typing import List, Dict, Optional, Any
import mlflow

# --- 关键导入：Feast 和我们自定义的数据源 ---
from feast import FeatureStore
from src.training.datasources import DATASOURCE_REGISTRY, TrainingDataSource

# --- 导入我们的 Ray 训练脚本 ---
from src.training.train import run_ray_training

# 假设 SparkSession 已经配置好
from pyspark.sql import SparkSession

def get_spark_session(appName: str) -> SparkSession:
    """
    获取或创建一个 SparkSession。
    此函数集中管理 Spark 连接配置，方便在不同环境间切换。
    """
    return SparkSession.builder.appName(appName).remote("sc://spark-connect-external-service:15002").getOrCreate()

# --- 任务定义区 (Tasks remain unchanged) ---

@task(name="Generate Training Data (Spark)")
def generate_training_dataset(
    flow_run_id: str,
    training_data_source: TrainingDataSource,
    feature_list: List[str],
    data_start_date: str,
    data_end_date: str,
    sampling_ratio: float,
    feature_repo_path: str = "feature_repo",
) -> str:
    # ... (此任务的内部逻辑保持不变)
    logger = get_run_logger()
    output_table_name = f"training_datasets.run_{flow_run_id}"
    logger.info(f"Generating unique training dataset: {output_table_name}")
    spark = get_spark_session(appName=f"GenerateTrainingDataset-{flow_run_id}")
    fs = FeatureStore(repo_path=feature_repo_path)
    try:
        output_db = "training_datasets"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {output_db}")
        logger.info(f"Loading entity dataframe from source '{training_data_source.name}'...")
        entity_df = training_data_source.get_entity_df(
            spark=spark,
            start_date=data_start_date,
            end_date=data_end_date,
            sampling_ratio=sampling_ratio
        )
        logger.info(f"Assembling training data with features: {feature_list}")
        training_df = fs.get_historical_features(
            entity_df=entity_df,
            features=feature_list,
        ).to_spark_df()
        logger.info("--- [DEBUG] Verifying top 10 rows of the final training DataFrame ---")
        training_df.show(10, truncate=False)
        logger.info(f"Saving final training dataset to {output_table_name}")
        training_df.write.mode("overwrite").saveAsTable(output_table_name)
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()
    return output_table_name

@task(name="Train Model (Ray)")
def train_model(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> dict:
    # ... (此任务的内部逻辑保持不变)
    logger = get_run_logger()
    mlflow_tracking_uri = prefect.variables.get("mlflow_tracking_uri")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    logger.info(f"MLflow tracking URI set to: {mlflow_tracking_uri}")
    logger.info(f"Starting model training on Ray using data from {training_data_table}")
    results = run_ray_training(
        training_data_table=training_data_table,
        mlflow_experiment_name=mlflow_experiment_name,
        run_parameters=run_parameters
    )
    return results

@task(name="Evaluate and Register Model")
def evaluate_and_register_model(
    training_results: dict, 
    evaluation_threshold: float,
    model_name: str
):
    # ... (此任务的内部逻辑保持不变)
    pass # 省略以保持简洁，实际代码已在历史记录中

# --- 子流程定义区 (Subflows for different execution environments) ---

@flow(name="Subflow: Generate Training Data")
def generate_training_data_flow(
    flow_run_id: str,
    data_source_name: str,
    feature_list: List[str],
    data_start_date: str,
    data_end_date: str,
    sampling_ratio: float,
) -> str:
    """这个子流程只负责数据生成，它将在标准的 Prefect Worker 上运行。"""
    training_data_source_obj = DATASOURCE_REGISTRY[data_source_name]
    return generate_training_dataset(
        flow_run_id=flow_run_id,
        training_data_source=training_data_source_obj,
        feature_list=feature_list,
        data_start_date=data_start_date,
        data_end_date=data_end_date,
        sampling_ratio=sampling_ratio
    )

@flow(
    name="Subflow: Train and Evaluate on Ray",
    task_runner=RayTaskRunner(address="ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001")
)
def model_training_ray_flow(
    training_data_table: str,
    mlflow_experiment_name: str,
    evaluation_threshold: float,
    run_parameters: Dict,
):
    """这个子流程负责模型训练和评估，它的所有任务都将在 Ray 上运行。"""
    training_results = train_model(
        training_data_table=training_data_table, 
        mlflow_experiment_name=mlflow_experiment_name,
        run_parameters=run_parameters
    )
    
    evaluate_and_register_model(
        training_results=training_results,
        evaluation_threshold=evaluation_threshold,
        model_name=mlflow_experiment_name
    )

# --- 主流程定义区 (The Main Entrypoint Flow) ---

# --- 关键修复：将 Flow 名称改回 Prefect 部署所期望的名称 ---
@flow(name="Versioned Model Training Flow")
def training_pipeline_flow(
    # --- 参数化 ---
    data_source_name: str = "movielens_ratings",
    data_start_date: str = "2019-01-01",
    data_end_date: str = "2019-01-31",
    feature_list: List[str] = [
        "user_rolling_features:avg_rating_past_30d",
        "user_rolling_features:rating_count_past_30d",
        "movie_static_features:genres"
    ],
    sampling_ratio: float = 1.0,
    model_hyperparameters: Dict[str, Any] = {"learning_rate": 0.01, "epochs": 5},
    evaluation_threshold: float = 0.85,
    mlflow_experiment_name: str = "movielens-recommendation-dev"
):
    """
    这是一个更高阶的、作为唯一入口的流程。
    它负责按顺序调用数据生成和模型训练这两个子流程。
    """
    ctx = get_run_context()
    flow_run_id = ctx.flow_run.id.hex if ctx.flow_run else "local_run_" + datetime.now().strftime("%Y%m%d%H%M%S")

    run_params = {
        "data_source_name": data_source_name,
        "data_start_date": data_start_date,
        "data_end_date": data_end_date,
        "feature_list": ", ".join(feature_list),
        "sampling_ratio": sampling_ratio,
        "evaluation_threshold": evaluation_threshold,
        "mlflow_experiment_name": mlflow_experiment_name,
        **model_hyperparameters
    }
    
    # 步骤 1: 运行数据生成子流程
    training_data_table = generate_training_data_flow(
        flow_run_id=flow_run_id,
        data_source_name=data_source_name,
        feature_list=feature_list,
        data_start_date=data_start_date,
        data_end_date=data_end_date,
        sampling_ratio=sampling_ratio
    )
    
    # 步骤 2: 运行模型训练子流程，并传入上一步的结果
    model_training_ray_flow(
        training_data_table=training_data_table,
        mlflow_experiment_name=mlflow_experiment_name,
        evaluation_threshold=evaluation_threshold,
        run_parameters=run_params
    )

