"""
本模块包含了使用 Ray Train 进行分布式模型训练的核心逻辑。
"""
import os
import torch
import torch.nn as nn
from typing import Dict, Any, List
import logging

import ray
import ray.train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from ray.air import session

import mlflow
from ray.data.preprocessors import StandardScaler, OneHotEncoder, Chain
from prefect import flow, task, get_run_logger
import pyarrow.fs

# --- 配置区 ---
HIVE_WAREHOUSE_PATH = os.getenv("HIVE_WAREHOUSE_PATH", "s3://spark-warehouse/")
RAY_CLUSTER_ADDRESS = os.getenv("RAY_CLUSTER_ADDRESS", "ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001")

def get_git_commit_hash() -> str:
    """获取当前的 Git Commit 哈希值，用于版本追溯。"""
    try:
        import subprocess
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
    except Exception:
        return "unknown"

def train_loop_per_worker(config: Dict):
    """
    在每个 Ray Worker 上执行的训练循环。
    """
    # 从配置中获取参数
    lr = config.get("learning_rate", 0.01)
    epochs = config.get("epochs", 5)
    batch_size = config.get("batch_size", 1024)
    
    # 获取由 Ray Train 分配的、已经预处理过的数据分片
    train_shard = session.get_dataset_shard("train")
    val_shard = session.get_dataset_shard("validation")
    
    # 从第一个批次中动态获取特征维度
    first_batch = next(train_shard.iter_batches(batch_size=1, dtypes=torch.float32))
    input_size = first_batch["features"].shape[1]

    # 定义模型和优化器
    model = nn.Sequential(
        nn.Linear(input_size, 64),
        nn.ReLU(),
        nn.Linear(64, 32),
        nn.ReLU(),
        nn.Linear(32, 1),
        nn.Sigmoid()
    )
    model = ray.train.torch.prepare_model(model)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # 训练循环
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

        # 验证循环
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

        # --- 使用 session.report() 将指标和检查点报告给 Ray Train ---
        session.report(
            {"loss": total_loss / num_batches, "val_accuracy": val_accuracy},
            checkpoint=ray.train.Checkpoint.from_dict(
                dict(epoch=epoch, model_state_dict=model.state_dict())
            ),
        )

@ray.remote
def log_on_worker(message: str):
    """这个函数会在一个 Ray worker 进程中执行。"""
    # 在 worker 端也需要配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER %(levelname)s - %(message)s')
    logging.info(message)
    return True


def run_ray_training(
    training_data_table: str,
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> Dict:
    """
    一个完整的 Ray 训练作业的入口函数。
    【关键修改】此函数现在负责连接和断开 Ray 集群。
    """
    logger = get_run_logger()
    logger.info(f"Connecting to Ray cluster at: {RAY_CLUSTER_ADDRESS}")
    ray.init(address=RAY_CLUSTER_ADDRESS, ignore_reinit_error=True)

    log_task_ref = log_on_worker.remote(f"Step 1: Preparing to read Parquet data from worker. table: {training_data_table}")
        # 2. ray.get() 会等待任务完成，确保日志已被打印
    ray.get(log_task_ref)

    logger.info(f"Finish remote logging")

    try:
        # --- 1. 设置 MLflow ---
        mlflow.set_experiment(mlflow_experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_params(run_parameters)
            mlflow.log_param("git_commit_hash", get_git_commit_hash())
            mlflow.log_param("training_data_table", training_data_table)

            db_name, table_name = training_data_table.split(".", 1)
            table_path = f"{db_name}.db/{table_name}/"

            # --- 2. 加载数据 ---
            full_path = os.path.join(HIVE_WAREHOUSE_PATH, table_path)


            logger.info(f"Reading data from Parquet path: {full_path}")
            S3_ENDPOINT_URL = "http://minio.default.svc.cluster.local:9000"
            S3_ACCESS_KEY = "cXFVWCBKY6xlUVjuc8Qk"
            S3_SECRET_KEY = "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY"
            s3_filesystem = pyarrow.fs.S3FileSystem(
                endpoint_override=S3_ENDPOINT_URL,
                access_key=S3_ACCESS_KEY,
                secret_key=S3_SECRET_KEY,
                scheme="http"  # 如果你的 MinIO Endpoint 是 http 而不是 https，这步很重要
            )
            dataset = ray.data.read_parquet(full_path, filesystem=s3_filesystem)


            # --- 3. 动态预处理 ---
            all_cols = dataset.columns()
            label_col = "is_liked"
            # 排除所有非特征列
            feature_cols = [
                c for c in all_cols if c not in 
                [label_col, "user_id", "movieId", "event_timestamp", "title"]
            ]

            schema = dataset.schema()

            type_mapping = {name: str(type) for name, type in zip(schema.names, schema.types)}

# 2. 使用这个我们自己创建的字典进行安全的查找
            numerical_cols = [c for c in feature_cols if type_mapping.get(c) in ['float32', 'float64', 'int64', 'int32']]
            categorical_cols = [c for c in feature_cols if type_mapping.get(c) == 'string']
            
            preprocessors = []
            if numerical_cols:
                preprocessors.append(StandardScaler(columns=numerical_cols))
            if categorical_cols:
                preprocessors.append(OneHotEncoder(columns=categorical_cols))

            preprocessor = Chain(*preprocessors) if preprocessors else None
            
            # 将所有特征合并到一个 "features" 向量中
            def merge_features(df):
                import pandas as pd
                df['features'] = df[feature_cols].values.tolist()
                return df.drop(columns=feature_cols)

            # --- 4. 设置 Ray Trainer ---
            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
                datasets={"train": dataset},
                dataset_config={
                    "train": ray.train.DataConfig(
                        split_at_fraction=0.8,
                        preprocessor=preprocessor,
                        transform=lambda ds: ds.map_batches(merge_features).map_batches(
                            lambda batch: {"features": torch.tensor(batch["features"], dtype=torch.float32), "label": torch.tensor(batch[label_col], dtype=torch.float32)}
                        )
                    )
                },
                train_loop_config={
                    "learning_rate": run_parameters.get("learning_rate", 0.01),
                    "epochs": run_parameters.get("epochs", 5),
                    "batch_size": run_parameters.get("batch_size", 1024),
                },
            )
            
            # --- 5. 运行训练并获取结果 ---
            result = trainer.fit()
            best_checkpoint_dict = result.best_checkpoints[0][0].to_dict()
            
            # --- 6. 记录结果到 MLflow ---
            mlflow.log_metrics({k: v for k, v in result.metrics.items() if isinstance(v, (int, float))})
            
            # 恢复模型并记录
            model_state = best_checkpoint_dict['model_state_dict']
            # 需要重新实例化模型结构以加载状态
            # 这部分逻辑应该与 train_loop_per_worker 中的模型定义一致
            first_row = dataset.take(1)[0]
            # 这里需要一个更健壮的方式来获取 input_size
            # 暂时用一个估算值
            temp_preprocessed = preprocessor.fit_transform(dataset.limit(1))
            input_size = len(temp_preprocessed.schema().names) - len([label_col, "user_id", "movieId", "event_timestamp", "title"])

            model_to_log = nn.Sequential(
                nn.Linear(input_size, 64), nn.ReLU(),
                nn.Linear(64, 32), nn.ReLU(),
                nn.Linear(32, 1), nn.Sigmoid()
            )
            model_to_log.load_state_dict(model_state)
            mlflow.pytorch.log_model(model_to_log, "model")

            return {"metrics": result.metrics, "model_uri": f"runs:/{run.info.run_id}/model"}
            
    finally:
        logger.info("Shutting down Ray connection.")
        ray.shutdown()

