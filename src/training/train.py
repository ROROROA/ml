import os
import torch
import torch.nn as nn
from typing import Dict, Any, List
import logging
import pandas as pd

import ray
import ray.train
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, Checkpoint, session
from ray.data.preprocessors import StandardScaler, OneHotEncoder, Chain, Concatenator

import mlflow
from prefect import get_run_logger
import pyarrow.fs

# --- 1. 配置区 ---
HIVE_WAREHOUSE_PATH = os.getenv("HIVE_WAREHOUSE_PATH", "s3://spark-warehouse/")
RAY_CLUSTER_ADDRESS = os.getenv("RAY_CLUSTER_ADDRESS", "ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001")

def get_git_commit_hash() -> str:
    """获取当前的 Git Commit 哈希值，用于版本追溯。"""
    try:
        import subprocess
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
    except Exception:
        return "unknown"

# --- 2. Ray Worker 端的训练循环 ---
def train_loop_per_worker(config: Dict):
    """
    在每个 Ray Worker 上执行的训练循环。
    【关键修改】数据分割现在在客户端进行，避免了Worker内部的AttributeError问题。
    """
    # 从配置中获取参数
    lr = config.get("learning_rate", 0.01)
    epochs = config.get("epochs", 5)
    batch_size = config.get("batch_size", 1024)
    
    # a. 获取已经分割好的训练和验证数据分片
    train_shard = session.get_dataset_shard("train")
    val_shard = session.get_dataset_shard("val")

    # b. 移除了在worker内部进行数据分割的代码，避免AttributeError
    
    # c. 从第一个批次中动态获取特征维度
    #    预处理器已经将所有特征合并到了 "features" 列
    first_batch = next(train_shard.iter_batches(batch_size=1, dtypes=torch.float32))
    input_size = first_batch["features"].shape[1]

    # d. 定义模型、损失函数和优化器
    model = nn.Sequential(
        nn.Linear(input_size, 64), nn.ReLU(),
        nn.Linear(64, 32), nn.ReLU(),
        nn.Linear(32, 1), nn.Sigmoid()
    )
    model = ray.train.torch.prepare_model(model)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # e. 训练和验证循环
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        num_batches = 0
        # 使用 train_shard 进行训练
        for batch in train_shard.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
            inputs = batch["features"]
            labels = batch["label"].view(-1, 1)
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            num_batches += 1

        model.eval()
        total_correct = 0
        total_samples = 0
        with torch.no_grad():
            # 使用 val_shard 进行验证
            for batch in val_shard.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
                inputs = batch["features"]
                labels = batch["label"]
                outputs = model(inputs).squeeze()
                predictions = (outputs > 0.5).float()
                total_correct += (predictions == labels).sum().item()
                total_samples += labels.size(0)
        
        val_accuracy = total_correct / total_samples if total_samples > 0 else 0

        # f. 报告指标和检查点
        session.report(
            {"loss": total_loss / num_batches, "val_accuracy": val_accuracy},
            checkpoint=Checkpoint.from_dict(
                dict(epoch=epoch, model_state_dict=model.state_dict())
            ),
        )

# --- 3. Prefect Task 调用的主函数 ---
def run_ray_training(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> Dict:
    """
    一个完整的 Ray 训练作业的入口函数。
    此函数在 Prefect worker 中作为 Ray Client 运行。
    """
    logger = get_run_logger()
    logger.info("--- EXECUTING FINAL COMPLETE VERSION OF TRAINING SCRIPT ---")
    logger.info(f"Connecting to Ray cluster at: {RAY_CLUSTER_ADDRESS}")
    
    # a. 手动管理 Ray 连接
    ray.init(address=RAY_CLUSTER_ADDRESS, ignore_reinit_error=True)

    try:
        # b. MLflow 设置
        mlflow.set_experiment(mlflow_experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_params(run_parameters)
            mlflow.log_param("git_commit_hash", get_git_commit_hash())
            mlflow.log_param("training_data_table", training_data_table)

            # c. 加载数据
            db_name, table_name = training_data_table.split(".", 1)
            table_path = f"{db_name}.db/{table_name}/"
            full_path = os.path.join(HIVE_WAREHOUSE_PATH, table_path)

            logger.info(f"Reading data from Parquet path: {full_path}")
            s3_filesystem = pyarrow.fs.S3FileSystem(
                endpoint_override=os.getenv("S3_ENDPOINT_URL", "http://minio.default.svc.cluster.local:9000"),
                access_key=os.getenv("S3_ACCESS_KEY", "cXFVWCBKY6xlUVjuc8Qk"),
                secret_key=os.getenv("S3_SECRET_KEY", "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY"),
                scheme="http"
            )
            dataset = ray.data.read_parquet(full_path, filesystem=s3_filesystem)
            logger.info(f"Successfully created Ray Dataset. Count: {dataset.count()}")

            # 在客户端进行数据分割，避免在Worker内部调用train_test_split导致的AttributeError
            logger.info("Splitting dataset into training and validation sets...")
            train_dataset, val_dataset = dataset.train_test_split(test_size=0.2, shuffle=True)
            logger.info(f"Dataset split complete. Train count: {train_dataset.count()}, Validation count: {val_dataset.count()}")

            # d. 定义预处理器
            all_cols = dataset.columns()
            label_col = "is_liked"
            feature_cols = [c for c in all_cols if c not in [label_col, "user_id", "movieId", "event_timestamp", "title"]]
            
            schema = dataset.schema()
            type_mapping = {name: str(dtype) for name, dtype in zip(schema.names, schema.types)}
            
            numerical_cols = [c for c in feature_cols if type_mapping.get(c) in ['float', 'double', 'int', 'long', 'float32', 'float64', 'int64', 'int32']]
            categorical_cols = [c for c in feature_cols if type_mapping.get(c) == 'string']
            
            preprocessors = []
            if numerical_cols:
                preprocessors.append(StandardScaler(columns=numerical_cols))
            if categorical_cols:
                preprocessors.append(OneHotEncoder(columns=categorical_cols))
            
            # 使用 Chain 将所有预处理器串联起来，并用 Concatenator 将结果合并为 "features" 向量
            preprocessor = Chain(*preprocessors, Concatenator(output_column_name="features", exclude=[label_col]))

            # e. 设置 Ray Trainer
            logger.info("Configuring TorchTrainer...")
            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
                # 传递已经分割好的数据集，避免在worker内部进行分割
                datasets={"train": train_dataset, "val": val_dataset},
                preprocessor=preprocessor,
                train_loop_config={
                    "learning_rate": run_parameters.get("learning_rate", 0.01),
                    "epochs": run_parameters.get("epochs", 5),
                    "batch_size": run_parameters.get("batch_size", 1024),
                },
            )
            
            # f. 运行训练
            logger.info("Starting trainer.fit()...")
            result = trainer.fit()
            logger.info("Training finished.")
            
            # g. 记录结果到 MLflow
            best_checkpoint_dict = result.best_checkpoints[0][0].to_dict()
            mlflow.log_metrics({k: v for k, v in result.metrics.items() if isinstance(v, (int, float))})
            
            model_state = best_checkpoint_dict['model_state_dict']
            
            # 从训练结果中安全地获取最终特征维度
            feature_vector_size = result.preprocessor.stats_['concatenator']['output_shapes']['features'][0]
            
            model_to_log = nn.Sequential(
                nn.Linear(feature_vector_size, 64), nn.ReLU(),
                nn.Linear(64, 32), nn.ReLU(),
                nn.Linear(32, 1), nn.Sigmoid()
            )
            model_to_log.load_state_dict(model_state)
            mlflow.pytorch.log_model(model_to_log, "model")

            return {"metrics": result.metrics, "model_uri": f"runs:/{run.info.run_id}/model"}
            
    finally:
        # h. 确保断开 Ray 连接
        logger.info("Shutting down Ray connection.")
        ray.shutdown()