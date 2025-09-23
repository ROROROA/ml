"""
模型训练、验证与注册的 Prefect 工作流。
这个流程被设计为可参数化和可版本化的，以确保每次训练的可追溯性。
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
import os
from datetime import datetime
from typing import List, Dict, Optional, Any

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
    # return SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
    return SparkSession.builder.appName(appName).remote("sc://spark-connect-external-service:15002").getOrCreate()
    # return SparkSession.builder.remote("sc://localhost:31002").getOrCreate()

@task
def generate_training_dataset(
    flow_run_id: str,
    training_data_source: TrainingDataSource,
    feature_list: List[str],
    data_start_date: str,
    data_end_date: str,
    sampling_ratio: float,
    feature_repo_path: str = "feature_repo",
) -> str:
    """
    【可版本化的智能组装任务】
    根据传入的参数（特征列表、时间范围），动态地创建训练数据集。
    """
    logger = get_run_logger()
    
    output_table_name = f"training_datasets.run_{flow_run_id}"
    logger.info(f"Generating unique training dataset: {output_table_name}")
    
    spark = get_spark_session(appName=f"GenerateTrainingDataset-{flow_run_id}")
    fs = FeatureStore(repo_path=feature_repo_path)

    try:
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
        ).to_spark()

        # --- 新增：验证步骤 ---
        # 在写入数据之前，打印最终生成的训练宽表的前10行，以便调试和验证。
        logger.info("--- [DEBUG] Verifying top 10 rows of the final training DataFrame ---")
        training_df.show(10, truncate=False)

        logger.info(f"Saving final training dataset to {output_table_name}")
        training_df.write.mode("overwrite").saveAsTable(output_table_name)
    
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()
        
    return output_table_name

@task
def train_model(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> dict:
    """
    调用 Ray 训练脚本，并将所有版本化参数记录到 MLflow。
    """
    logger = get_run_logger()
    logger.info(f"Starting model training on Ray using data from {training_data_table}")
    
    results = run_ray_training(
        training_data_table=training_data_table,
        mlflow_experiment_name=mlflow_experiment_name,
        run_parameters=run_parameters
    )
    return results

@task
def evaluate_and_register_model(training_results: dict, evaluation_threshold: float):
    """
    评估模型并决定是否将其注册到生产环境。
    """
    logger = get_run_logger()
    val_accuracy = training_results.get("metrics", {}).get("val_accuracy", 0)
    model_uri = training_results.get("model_uri")
    
    logger.info(f"New model validation accuracy: {val_accuracy:.4f}")
    logger.info(f"Evaluation threshold is: {evaluation_threshold}")

    if val_accuracy > evaluation_threshold:
        logger.info("Model performance meets the criteria. Registering model...")
        # TODO: 在这里加入与生产模型比较的逻辑, 并调用 MLflow API 来注册模型
    else:
        logger.warning("Model performance did not meet the criteria. Skipping registration.")


@flow(name="Versioned Model Training Flow")
def training_pipeline_flow(
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
    一个完全可参数化、可版本化的模型训练工作流。
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
        **model_hyperparameters
    }

    training_data_source_obj = DATASOURCE_REGISTRY[data_source_name]
    
    training_data_table = generate_training_dataset(
        flow_run_id=flow_run_id,
        training_data_source=training_data_source_obj,
        feature_list=feature_list,
        data_start_date=data_start_date,
        data_end_date=data_end_date,
        sampling_ratio=sampling_ratio
    )
    
    training_results = train_model(
        training_data_table=training_data_table, 
        mlflow_experiment_name=mlflow_experiment_name,
        run_parameters=run_params
    )
    
    evaluate_and_register_model(
        training_results=training_results,
        evaluation_threshold=evaluation_threshold
    )

