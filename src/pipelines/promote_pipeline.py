"""
本模块定义了一个用于“提拔”模型到生产环境的 Prefect 工作流。
这是连接手动实验与自动化生产的关键桥梁。
"""
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation
import mlflow
import os
from typing import Dict, Any

# --- 配置 MLflow ---
# 在生产环境中，这应该指向你的 MLflow Tracking Server
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

@task
def get_run_info_from_mlflow(run_id: str) -> Dict[str, Any]:
    """
    根据 run_id 从 MLflow 中获取实验的所有关键信息。
    """
    logger = get_run_logger()
    logger.info(f"Fetching details for MLflow run_id: {run_id}")
    
    client = mlflow.tracking.MlflowClient()
    try:
        run = client.get_run(run_id)
        
        # 提取模型产出物的路径
        model_uri = f"runs:/{run_id}/model"
        
        logger.info("Successfully fetched run parameters and model URI.")
        return {
            "params": run.data.params,
            "model_uri": model_uri,
            "model_name": run.data.params.get("mlflow_experiment_name", "recommendation-model")
        }
    except Exception as e:
        logger.error(f"Failed to fetch run info from MLflow: {e}")
        raise

@task
def promote_model_in_registry(model_name: str, model_uri: str, stage: str = "Production"):
    """
    将指定的模型版本注册并提拔到生产（Production）阶段。
    """
    logger = get_run_logger()
    logger.info(f"Promoting model '{model_name}' to '{stage}' stage.")
    
    client = mlflow.tracking.MlflowClient()
    
    # 1. 注册模型（如果尚不存在）
    try:
        client.create_registered_model(model_name)
    except mlflow.exceptions.RestException:
        logger.info(f"Registered model '{model_name}' already exists.")

    # 2. 创建一个新的模型版本
    model_version = client.create_model_version(
        name=model_name,
        source=model_uri,
        run_id=model_uri.split('/')[1]
    )

    # 3. 将这个新版本提拔到指定阶段
    client.transition_model_version_stage(
        name=model_name,
        version=model_version.version,
        stage=stage,
        archive_existing_versions=True # 将之前的所有 Production 版本归档
    )
    logger.info(f"Successfully promoted model '{model_name}' version {model_version.version} to '{stage}'.")

@task
def create_or_update_scheduled_deployments(golden_params: Dict[str, Any]):
    """
    使用“黄金参数”来创建或更新生产环境的自动化调度部署。
    """
    logger = get_run_logger()
    logger.info("Creating or updating scheduled deployments in Prefect...")

    # --- 1. 配置每日数据管道调度 ---
    daily_data_command = (
        "prefect deploy src/pipelines/data_pipeline.py:data_pipeline_flow "
        "-n production-daily-features " # 部署名称
        "--cron '0 1 * * *' " # 每日凌晨1点
        "--apply" # 立即应用
    )
    logger.info(f"Deploying daily data pipeline with command:\n{daily_data_command}")
    ShellOperation(commands=[daily_data_command], stream_output=True).run()

    # --- 2. 配置每周训练管道调度 ---
    # 动态构建包含所有黄金参数的 prefect deploy 命令
    weekly_training_command_parts = [
        "prefect deploy src/pipelines/training_pipeline.py:training_pipeline_flow",
        "-n production-weekly-retraining", # 部署名称
        "--cron '0 2 * * 0'", # 每周日凌晨2点
    ]
    # 将从 MLflow 获取的参数附加到命令中
    for key, value in golden_params.items():
        # Prefect 的 --param 标志需要正确的引号处理
        param_value = f"'{value}'" if isinstance(value, str) else str(value)
        weekly_training_command_parts.append(f"--param {key}={param_value}")
    
    weekly_training_command_parts.append("--apply")
    weekly_training_command = " ".join(weekly_training_command_parts)
    
    logger.info(f"Deploying weekly retraining pipeline with command:\n{weekly_training_command}")
    ShellOperation(commands=[weekly_training_command], stream_output=True).run()

    logger.info("All production deployments have been successfully scheduled and activated.")


@flow
def promote_model_to_production_flow(mlflow_run_id_to_promote: str):
    """
    一个用于将实验模型“提拔”到生产环境的端到端工作流。
    """
    logger = get_run_logger()
    logger.info("Starting model promotion pipeline...")

    # 步骤 1: 从 MLflow 获取最佳实验的所有信息
    run_info = get_run_info_from_mlflow(run_id=mlflow_run_id_to_promote)

    # 步骤 2: 在 MLflow Model Registry 中提拔该模型
    promote_model_in_registry(
        model_name=run_info["model_name"],
        model_uri=run_info["model_uri"]
    )
    
    # 步骤 3: 使用该实验的“黄金参数”来配置并激活 Prefect 的自动化调度
    create_or_update_scheduled_deployments(golden_params=run_info["params"])
    
    logger.info("Model promotion and production scheduling completed successfully!")
