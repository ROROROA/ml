# src/training/train.py

import os
import torch
import torch.nn as nn
from typing import Dict, Any, List
import logging
import pandas as pd # 确保导入 pandas

import ray
import ray.train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from ray.air import session, Checkpoint # 导入 Checkpoint

import mlflow
from ray.data.preprocessors import StandardScaler, OneHotEncoder, Chain
from prefect import get_run_logger
import pyarrow.fs

# --- 配置区 (保持不变) ---
HIVE_WAREHOUSE_PATH = os.getenv("HIVE_WAREHOUSE_PATH", "s3://spark-warehouse/")
RAY_CLUSTER_ADDRESS = os.getenv("RAY_CLUSTER_ADDRESS", "ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001")

def get_git_commit_hash() -> str:
    """获取当前的 Git Commit 哈希值，用于版本追溯。"""
    try:
        import subprocess
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
    except Exception:
        return "unknown"

# --- train_loop_per_worker (保持不变，但为了完整性在此列出) ---
def train_loop_per_worker(config: Dict):
    """
    在每个 Ray Worker 上执行的训练循环。
    """
    lr = config.get("learning_rate", 0.01)
    epochs = config.get("epochs", 5)
    batch_size = config.get("batch_size", 1024)
    
    # 获取由 Ray Train 分配的、已经预处理过的数据分片
    train_shard = session.get_dataset_shard("train")
    val_shard = session.get_dataset_shard("validation")
    
    first_batch = next(train_shard.iter_batches(batch_size=1, dtypes=torch.float32))
    # 【重要】预处理器会把特征合并到 'features' 列
    input_size = first_batch["features"].shape[1]

    model = nn.Sequential(
        nn.Linear(input_size, 64), nn.ReLU(),
        nn.Linear(64, 32), nn.ReLU(),
        nn.Linear(32, 1), nn.Sigmoid()
    )
    model = ray.train.torch.prepare_model(model)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    for epoch in range(epochs):
        model.train()
        total_loss = 0
        num_batches = 0
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
            for batch in val_shard.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
                inputs = batch["features"]
                labels = batch["label"]
                
                outputs = model(inputs).squeeze()
                predictions = (outputs > 0.5).float()
                
                total_correct += (predictions == labels).sum().item()
                total_samples += labels.size(0)
        
        val_accuracy = total_correct / total_samples if total_samples > 0 else 0

        session.report(
            {"loss": total_loss / num_batches, "val_accuracy": val_accuracy},
            checkpoint=Checkpoint.from_dict(
                dict(epoch=epoch, model_state_dict=model.state_dict())
            ),
        )

# --- run_ray_training (核心修改区) ---
def run_ray_training(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> Dict:
    """
    一个完整的 Ray 训练作业的入口函数。
    此函数现在负责连接和断开 Ray 集群。
    """
    logger = get_run_logger()
    logger.info(f"Connecting to Ray cluster at: {RAY_CLUSTER_ADDRESS}")
    ray.init(address=RAY_CLUSTER_ADDRESS, ignore_reinit_error=True)

    try:
        # --- 1. MLflow 设置 ---
        mlflow.set_experiment(mlflow_experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_params(run_parameters)
            mlflow.log_param("git_commit_hash", get_git_commit_hash())
            mlflow.log_param("training_data_table", training_data_table)

            # --- 2. 加载数据 ---
            db_name, table_name = training_data_table.split(".", 1)
            table_path = f"{db_name}.db/{table_name}/"
            full_path = os.path.join(HIVE_WAREHOUSE_PATH, table_path)

            logger.info(f"Reading data from Parquet path: {full_path}")
            S3_ENDPOINT_URL = "http://minio.default.svc.cluster.local:9000"
            S3_ACCESS_KEY = "cXFVWCBKY6xlUVjuc8Qk"
            S3_SECRET_KEY = "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY"
            s3_filesystem = pyarrow.fs.S3FileSystem(
                endpoint_override=S3_ENDPOINT_URL,
                access_key=S3_ACCESS_KEY,
                secret_key=S3_SECRET_KEY,
                scheme="http"
            )
            dataset = ray.data.read_parquet(full_path, filesystem=s3_filesystem)
            logger.info(f"Successfully created Ray Dataset. Count: {dataset.count()}")
            
            # 【关键修改】在这里执行数据集分割
            logger.info("Splitting dataset into training and validation sets...")
            train_dataset, validation_dataset = dataset.train_test_split(test_size=0.2, shuffle=True)
            logger.info(f"Dataset split complete. Train count: {train_dataset.count()}, Validation count: {validation_dataset.count()}")

            # --- 3. 动态预处理 ---
            all_cols = dataset.columns()
            label_col = "is_liked"
            feature_cols = [
                c for c in all_cols if c not in 
                [label_col, "user_id", "movieId", "event_timestamp", "title"]
            ]
            
            schema = dataset.schema()
            type_mapping = {name: str(dtype) for name, dtype in zip(schema.names, schema.types)}
            
            numerical_cols = [c for c in feature_cols if type_mapping.get(c) in ['float', 'double', 'int', 'long', 'float32', 'float64', 'int64', 'int32']]
            categorical_cols = [c for c in feature_cols if type_mapping.get(c) == 'string']
            
            preprocessors = []
            if numerical_cols:
                preprocessors.append(StandardScaler(columns=numerical_cols))
            if categorical_cols:
                # 合并所有分类特征到 'features' 中
                preprocessors.append(OneHotEncoder(columns=categorical_cols))
            
            # 将所有处理过的特征合并到一个名为 'features' 的向量中
            # Concatenator 会取 StandardScaler 和 OneHotEncoder 的输出列并将它们合并
            preprocessor = Chain(*preprocessors, Concatenator(output_column_name="features", include=[f"scaler_({c})" for c in numerical_cols] + [f"one_hot_encoder({c})" for c in categorical_cols]))

            # --- 4. 设置 Ray Trainer ---
            logger.info("Configuring TorchTrainer...")
            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
                datasets={
                    "train": train_dataset,
                    "validation": validation_dataset
                },
                preprocessor=preprocessor, # 【关键修改】直接在这里传递预处理器
                train_loop_config={
                    "learning_rate": run_parameters.get("learning_rate", 0.01),
                    "epochs": run_parameters.get("epochs", 5),
                    "batch_size": run_parameters.get("batch_size", 1024),
                },
            )
            
            # --- 5. 运行训练 ---
            logger.info("Starting trainer.fit()...")
            result = trainer.fit()
            logger.info("Training finished.")
            
            # --- 6. 记录结果到 MLflow ---
            best_checkpoint_dict = result.best_checkpoints[0][0].to_dict()
            mlflow.log_metrics({k: v for k, v in result.metrics.items() if isinstance(v, (int, float))})
            
            model_state = best_checkpoint_dict['model_state_dict']
            
            # 【关键修改】更健壮地获取输入维度
            # 从预处理器的统计信息中获取最终 'features' 列的维度
            preprocessor_stats = result.preprocessor.stats_
            feature_vector_size = preprocessor_stats['concatenator']['output_shapes']['features']
            
            model_to_log = nn.Sequential(
                nn.Linear(feature_vector_size, 64), nn.ReLU(),
                nn.Linear(64, 32), nn.ReLU(),
                nn.Linear(32, 1), nn.Sigmoid()
            )
            model_to_log.load_state_dict(model_state)
            mlflow.pytorch.log_model(model_to_log, "model")

            return {"metrics": result.metrics, "model_uri": f"runs:/{run.info.run_id}/model"}
            
    finally:
        logger.info("Shutting down Ray connection.")
        ray.shutdown()