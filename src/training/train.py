# src/training/train.py

import os
from typing import Dict, Any, List
import logging
import pandas as pd # 确保导入 pandas
import numpy as np
import pickle
import tempfile

import ray

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

@ray.remote(num_cpus=1)
def worker_train_task(config: Dict) -> Dict[str, Any]:
    lr = float(config.get("learning_rate", 0.1))
    epochs = int(config.get("epochs", 10))
    batch_size = int(config.get("batch_size", 4096))
    max_rows = int(config.get("max_rows", 500000))

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
    if max_rows > 0 and len(df) > max_rows:
        df = df.head(max_rows)

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

    # 简单标准化有助于收敛
    feature_means = X.mean(axis=0, keepdims=True)
    feature_stds = X.std(axis=0, keepdims=True) + 1e-6
    X = (X - feature_means) / feature_stds

    # 切分（固定随机种子）
    rng = np.random.RandomState(42)
    indices = rng.permutation(len(X))
    train_end = int(0.8 * len(X))
    train_idx, val_idx = indices[:train_end], indices[train_end:]
    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]

    # 纯 NumPy 逻辑回归（二分类，交叉熵损失）
    def sigmoid(z: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-z))

    num_features = X_train.shape[1]
    weights = np.zeros((num_features, 1), dtype=np.float32)
    bias = 0.0

    num_samples = X_train.shape[0]
    for _ in range(epochs):
        for start in range(0, num_samples, batch_size):
            end = min(start + batch_size, num_samples)
            xb = X_train[start:end]
            yb = y_train[start:end]
            logits = xb @ weights + bias
            probs = sigmoid(logits)
            grad_w = xb.T @ (probs - yb) / (end - start)
            grad_b = float((probs - yb).mean())
            weights -= lr * grad_w
            bias -= lr * grad_b

    # 验证
    val_logits = X_val @ weights + bias
    val_probs = sigmoid(val_logits).reshape(-1)
    val_preds = (val_probs > 0.5).astype(np.float32)
    val_labels = y_val.reshape(-1)
    val_acc = float((val_preds == val_labels).mean())
    # 数值稳定 logloss
    eps = 1e-7
    ll = float(-(val_labels * np.log(val_probs + eps) + (1 - val_labels) * np.log(1 - val_probs + eps)).mean())

    model_payload = {
        "weights": weights,
        "bias": bias,
        "feature_names": numeric_cols,
        "feature_means": feature_means,
        "feature_stds": feature_stds,
    }
    return {"metrics": {"loss": ll, "val_accuracy": val_acc}, "model_bytes": pickle.dumps(model_payload)}

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
            # 不在运行时安装大型依赖，避免 raylet 异常
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

            # --- 3. 单 worker 远程任务训练 ---
            logger.info("Submitting XGBoost single-worker training task...")
            task_config = {
                "learning_rate": run_parameters.get("learning_rate", 0.1),
                "epochs": run_parameters.get("epochs", 1),
                "batch_size": run_parameters.get("batch_size", 1024),
                "s3_endpoint": S3_ENDPOINT_URL,
                "s3_access_key": S3_ACCESS_KEY,
                "s3_secret_key": S3_SECRET_KEY,
                "parquet_path": full_path,
            }
            result = ray.get(worker_train_task.remote(task_config))
            logger.info("Training finished.")
            
            # --- 6. 记录结果到 MLflow ---
            # 记录结果到 MLflow（指标 + 作为 artifact 的模型权重）
            metrics = result.get("metrics", {})
            mlflow.log_metrics({k: float(v) for k, v in metrics.items() if isinstance(v, (int, float))})

            model_bytes = result["model_bytes"]
            with tempfile.TemporaryDirectory() as td:
                model_path = os.path.join(td, "model.pkl")
                with open(model_path, "wb") as f:
                    f.write(model_bytes)
                mlflow.log_artifact(model_path, artifact_path="model")

            return {"metrics": metrics, "model_uri": f"runs:/{run.info.run_id}/model"}
            
    finally:
        logger.info("Shutting down Ray connection.")
        ray.shutdown()