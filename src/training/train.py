# src/training/train.py

import os
from typing import Dict, Any, List
import logging
import pandas as pd # 确保导入 pandas
import numpy as np
import pickle

import ray
import ray.train
from ray.train.sklearn import SklearnTrainer
from ray.air.config import ScalingConfig
from ray.train import session, Checkpoint 

import mlflow
from prefect import get_run_logger
import pyarrow.fs
import pyarrow.dataset as ds

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
    【关键修改】数据分割现在在客户端进行，避免了Worker内部的AttributeError问题。
    """
    lr = config.get("learning_rate", 0.01)
    epochs = config.get("epochs", 5)
    batch_size = config.get("batch_size", 1024)

    # 在 worker 内直接从 S3 读取数据，避免 Ray Dataset 计划执行
    s3_endpoint = config["s3_endpoint"]
    s3_access_key = config["s3_access_key"]
    s3_secret_key = config["s3_secret_key"]
    parquet_path = config["parquet_path"]

    s3_filesystem = pyarrow.fs.S3FileSystem(
        endpoint_override=s3_endpoint,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        scheme="http"
    )

    pa_dataset = ds.dataset(parquet_path, filesystem=s3_filesystem, format="parquet")
    table = pa_dataset.to_table()
    df = table.to_pandas()

    # 简化：仅使用数值特征，标签列为 is_liked
    label_col = "is_liked"
    drop_cols = {label_col, "user_id", "movieId", "event_timestamp", "title"}
    numeric_cols = [c for c, dt in zip(table.schema.names, table.schema.types)
                    if c not in drop_cols and str(dt) in [
                        'int8','int16','int32','int64','uint8','uint16','uint32','uint64',
                        'float32','float64','float','double','decimal128(38,0)'
                    ]]

    df = df[[*numeric_cols, label_col]].dropna()
    X = df[numeric_cols].astype(np.float32).values
    y = df[label_col].astype(np.float32).values.reshape(-1, 1)

    # 切分（固定随机种子）
    rng = np.random.RandomState(42)
    indices = rng.permutation(len(X))
    train_end = int(0.8 * len(X))
    train_idx, val_idx = indices[:train_end], indices[train_end:]
    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]

    # 使用 sklearn 逻辑回归（最简依赖）
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import log_loss, accuracy_score

    model = LogisticRegression(max_iter=200, n_jobs=1)

    # 简单轮次：每轮完整拟合一次（LogisticRegression 本身是批优化）
    for epoch in range(epochs):
        model.fit(X_train, y_train.ravel())
        # 验证指标
        proba = model.predict_proba(X_val)[:, 1]
        preds = (proba > 0.5).astype(np.float32)
        val_acc = accuracy_score(y_val, preds)
        ll = log_loss(y_val, proba, labels=[0, 1])

        session.report(
            {"loss": float(ll), "val_accuracy": float(val_acc)},
            checkpoint=Checkpoint.from_dict(
                {"epoch": epoch, "model_bytes": pickle.dumps(model)}
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

    # 将当前项目代码目录下发给 Ray worker，保证能导入相同模块
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    ray.init(
        address=RAY_CLUSTER_ADDRESS,
        ignore_reinit_error=True,
        runtime_env={
            "working_dir": project_root,
        },
    )

    try:
        # --- 1. MLflow 设置 ---
        mlflow.set_experiment(mlflow_experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_params(run_parameters)
            mlflow.log_param("git_commit_hash", get_git_commit_hash())
            mlflow.log_param("training_data_table", training_data_table)

            # --- 2. 仅构造路径与访问配置，数据读取放到 worker 内 ---
            db_name, table_name = training_data_table.split(".", 1)
            table_path = f"{db_name}.db/{table_name}/"
            full_path = os.path.join(HIVE_WAREHOUSE_PATH, table_path)

            logger.info(f"Training path (Parquet on S3): {full_path}")
            S3_ENDPOINT_URL = "http://minio.default.svc.cluster.local:9000"
            S3_ACCESS_KEY = "cXFVWCBKY6xlUVjuc8Qk"
            S3_SECRET_KEY = "Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY"

            # --- 3. 设置 Ray Trainer（最简：单 worker，不使用 Ray Dataset）---
            logger.info("Configuring SklearnTrainer (single worker, no Ray Dataset)...")
            trainer = SklearnTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
                train_loop_config={
                    "learning_rate": run_parameters.get("learning_rate", 0.01),
                    "epochs": run_parameters.get("epochs", 5),
                    "batch_size": run_parameters.get("batch_size", 1024),
                    "s3_endpoint": S3_ENDPOINT_URL,
                    "s3_access_key": S3_ACCESS_KEY,
                    "s3_secret_key": S3_SECRET_KEY,
                    "parquet_path": full_path,
                },
            )
            
            # --- 5. 运行训练 ---
            logger.info("Starting trainer.fit()...")
            result = trainer.fit()
            logger.info("Training finished.")
            
            # --- 6. 记录结果到 MLflow ---
            best_checkpoint_dict = result.best_checkpoints[0][0].to_dict()
            mlflow.log_metrics({k: v for k, v in result.metrics.items() if isinstance(v, (int, float))})
            
            # 反序列化 sklearn 模型并记录到 MLflow
            model_bytes = best_checkpoint_dict['model_bytes']
            model_obj = pickle.loads(model_bytes)
            import mlflow.sklearn
            mlflow.sklearn.log_model(model_obj, "model")

            return {"metrics": result.metrics, "model_uri": f"runs:/{run.info.run_id}/model"}
            
    finally:
        logger.info("Shutting down Ray connection.")
        ray.shutdown()