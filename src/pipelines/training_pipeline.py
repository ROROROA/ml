"""
模型训练、验证与注册的 Prefect 工作流。
这个流程被设计为可参数化和可版本化的，以确保每次训练的可追溯性。
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
# --- 关键导入：引入 RayTaskRunner 和 ConcurrentTaskRunner ---
from prefect_ray.task_runners import RayTaskRunner
from prefect.task_runners import ConcurrentTaskRunner
# --- 关键修复：修改 Variable 的导入方式 ---
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
    # return SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
    return SparkSession.builder.appName(appName).remote("sc://spark-connect-external-service:15002").getOrCreate()
    # return SparkSession.builder.remote("sc://localhost:31002").getOrCreate()

# --- 关键修改：为 Spark 任务指定一个非 Ray 的 Task Runner ---
@task(
    name="Generate Training Data (Spark)",
    description="使用 Spark 和 Feast 生成时间点正确的训练数据集。",
    # 这个任务将在执行它的 Prefect Worker 的本地线程池中运行，而不会被提交到 Ray。
    task_runner=ConcurrentTaskRunner() 
)
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
        # 确保目标数据库存在
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

# 这个任务没有指定 task_runner，因此它会使用 Flow 级别的默认设置（RayTaskRunner）
@task(name="Train Model (Ray)")
def train_model(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> dict:
    logger = get_run_logger()
    
    # --- 关键修复：使用新的 prefect.variables API ---
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
    """
    【完整版】
    评估模型并决定是否将其注册到生产环境。
    """
    logger = get_run_logger()
    
    # --- 关键修复：使用新的 prefect.variables API ---
    mlflow_tracking_uri = prefect.variables.get("mlflow_tracking_uri")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    logger.info(f"MLflow tracking URI set for evaluation: {mlflow_tracking_uri}")
    
    val_accuracy = training_results.get("metrics", {}).get("val_accuracy", 0)
    model_uri = training_results.get("model_uri")
    
    if not model_uri:
        logger.error("Model URI not found in training results. Skipping registration.")
        return

    run_id = model_uri.split('/')[1]
    
    logger.info(f"New model validation accuracy: {val_accuracy:.4f}")
    logger.info(f"Evaluation threshold is: {evaluation_threshold}")

    if val_accuracy > evaluation_threshold:
        logger.info(f"Model performance meets the criteria. Registering and promoting model '{model_name}'...")
        try:
            client = mlflow.tracking.MlflowClient()
            client.create_registered_model(model_name, exist_ok=True)
            model_version = client.create_model_version(
                name=model_name,
                source=model_uri,
                run_id=run_id
            )
            client.transition_model_version_stage(
                name=model_name,
                version=model_version.version,
                stage="Production",
                archive_existing_versions=True
            )
            logger.info(f"Successfully promoted model '{model_name}' version {model_version.version} to 'Production'.")
        except Exception as e:
            logger.error(f"Failed to register or promote model in MLflow: {e}")
            raise
    else:
        logger.warning("Model performance did not meet the criteria. Skipping registration.")


@flow(
    name="Versioned Model Training Flow (Ray Backend)",
    task_runner=RayTaskRunner(address="ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001")
)
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
        evaluation_threshold=evaluation_threshold,
        model_name=mlflow_experiment_name
    )

