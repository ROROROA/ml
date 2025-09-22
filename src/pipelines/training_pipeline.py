"""
模型训练、验证与注册的 Prefect 工作流。
这个流程被设计为可参数化和可版本化的，以确保每次训练的可追溯性。
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
import os
from datetime import datetime
from typing import List, Dict, Optional

# --- 关键导入：Feast 和我们新定义的数据源 ---
from feast import FeatureStore
from src.training.datasources import TrainingDataSource, DATASOURCE_REGISTRY

# --- 导入我们之前定义的 Ray 训练脚本 ---
from src.training.train import run_ray_training

# 假设 SparkSession 已经配置好
from pyspark.sql import SparkSession

@task
def generate_training_dataset(
    training_data_source: TrainingDataSource, # 参数化：传入一个数据源对象
    feature_repo_path: str,
    feature_list: List[str],
    data_start_date: str,
    data_end_date: str,
    sampling_ratio: Optional[float] = None
) -> str:
    """
    【可版本化的智能组装任务】
    根据传入的参数（数据源、特征列表、时间范围、采样率），动态地创建训练数据集。
    """
    logger = get_run_logger()
    ctx = get_run_context()
    
    # 生成唯一的输出表名以确保数据版本化
    unique_run_id = ctx.flow_run.id.hex
    output_table_name = f"training_datasets.run_{unique_run_id}"
    
    logger.info(f"Generating unique training dataset: {output_table_name}")
    
    spark = SparkSession.builder.appName(f"GenerateTrainingDataset-{unique_run_id}").getOrCreate()
    fs = FeatureStore(repo_path=feature_repo_path)

    # 1. 使用传入的 training_data_source 对象来获取“实体时间戳”
    logger.info(f"Loading entity spine using '{training_data_source.name}' data source.")
    entity_df = training_data_source.get_dataframe(spark, data_start_date, data_end_date)

    # 如果设置了采样率，则对数据进行采样
    if sampling_ratio and 0 < sampling_ratio < 1:
        logger.info(f"Sampling data with ratio: {sampling_ratio}")
        entity_df = entity_df.sample(withReplacement=False, fraction=sampling_ratio)

    # 2. 使用参数化的特征列表
    logger.info(f"Assembling training data with features: {feature_list}")

    # 3. 调用 get_historical_features 进行智能 Join
    training_df = fs.get_historical_features(
        entity_df=entity_df,
        features=feature_list,
    ).to_spark()

    # 4. 保存唯一的、不可变的训练数据集
    logger.info(f"Saving final training dataset to {output_table_name}")
    training_df.write.mode("overwrite").saveAsTable(output_table_name)
    
    spark.stop()
    return output_table_name

@task
def train_model(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> dict:
    # ... (此任务保持不变)
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
    # ... (此任务保持不变)
    logger = get_run_logger()
    val_accuracy = training_results.get("metrics", {}).get("val_accuracy", 0)
    model_uri = training_results.get("model_uri")
    
    logger.info(f"New model validation accuracy: {val_accuracy:.4f}")
    logger.info(f"Evaluation threshold is: {evaluation_threshold}")

    if val_accuracy > evaluation_threshold:
        logger.info("Model performance meets the criteria. Registering model...")
        # TODO: 在这里调用 MLflow API 来注册模型
    else:
        logger.warning("Model performance did not meet the criteria. Skipping registration.")

@flow(name="Versioned Model Training Flow")
def training_pipeline_flow(
    # Data parameters
    data_source_name: str = "historical_interactions", # <--- 参数现在是字符串
    data_start_date: str = "2023-01-01",
    data_end_date: str = "2023-06-30",
    sampling_ratio: Optional[float] = None,

    # Feature parameters
    feature_list: List[str] = [
        "user_rolling_features:avg_purchase_amount_3d",
        "user_realtime_features:page_views_10m"
    ],

    # Model parameters
    model_hyperparameters: Dict = {"learning_rate": 0.01, "epochs": 5},
    
    # MLOps parameters
    mlflow_experiment_name: str = "recommendation-model-v2",
    evaluation_threshold: float = 0.85
):
    """
    一个完全可参数化、可版本化的模型训练工作流。
    """
    logger = get_run_logger()
    logger.info("Starting training pipeline with full parameterization...")

    # --- 在 Flow 内部，根据字符串名称从注册表中查找数据源对象 ---
    if data_source_name not in DATASOURCE_REGISTRY:
        raise ValueError(f"Data source '{data_source_name}' not found in DATASOURCE_REGISTRY.")
    training_data_source_obj = DATASOURCE_REGISTRY[data_source_name]
    
    # 准备好要记录到 MLflow 的所有参数
    run_params = {
        "data_source_name": data_source_name, # 记录我们使用的源名称
        "data_start_date": data_start_date,
        "data_end_date": data_end_date,
        "sampling_ratio": sampling_ratio,
        "features": ", ".join(feature_list),
        **model_hyperparameters
    }

    training_data_table = generate_training_dataset(
        training_data_source=training_data_source_obj, # <--- 在这里传入查找到的数据源对象
        feature_repo_path="feature_repo",
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

