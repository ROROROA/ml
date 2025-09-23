"""
本模块包含了使用 Ray Train 进行分布式模型训练的核心逻辑。
"""
import os
import torch
import torch.nn as nn
from typing import Dict, Any, List

import ray
import ray.train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from ray.air import session

import mlflow
from sklearn.model_selection import train_test_split
from ray.data.preprocessors import StandardScaler, OneHotEncoder

# --- 配置区 ---
# 在生产环境中，这应该通过环境变量或配置文件来管理
HIVE_WAREHOUSE_PATH = os.getenv("HIVE_WAREHOUSE_PATH", "s3a://spark-warehouse/")


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
    input_size = config.get("input_size") # 这是动态计算后传入的

    # 获取由 Ray Train 分配的、已经预处理过的数据分片
    train_shard = session.get_dataset_shard("train")
    val_shard = session.get_dataset_shard("validation")

    # 定义模型和优化器
    model = nn.Sequential(
        nn.Linear(input_size, 64),
        nn.ReLU(),
        nn.Linear(64, 32),
        nn.ReLU(),
        nn.Linear(32, 1),
        nn.Sigmoid()
    )
    model = ray.train.torch.prepare_model(model) # Ray Train 必需的封装
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # 训练循环
    for epoch in range(epochs):
        model.train()
        for batch in train_shard.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
            inputs = batch["features"]
            labels = batch["label"].view(-1, 1) # 确保标签维度正确
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

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
            {"loss": loss.item(), "val_accuracy": val_accuracy},
            checkpoint=ray.train.Checkpoint.from_dict(
                dict(epoch=epoch, model=model.state_dict())
            ),
        )

def run_ray_training(
    training_data_table: str, # <-- 关键修复：添加此参数
    mlflow_experiment_name: str,
    run_parameters: Dict
) -> Dict:
    """
    一个完整的 Ray 训练作业的入口函数。
    """
    # --- 1. 设置 MLflow ---
    mlflow.set_experiment(mlflow_experiment_name)
    with mlflow.start_run() as run:
        mlflow.log_params(run_parameters)
        mlflow.log_param("git_commit_hash", get_git_commit_hash())
        mlflow.log_param("training_data_table", training_data_table)

        # --- 2. 加载数据 ---
        # 构造指向 Hive 表底层 Parquet 文件的完整路径
        # 注意：这里的 .replace(".", ".db/") 是一个常见的约定，请根据你的 Hive 配置调整
        full_path = os.path.join(HIVE_WAREHOUSE_PATH, training_data_table.replace(".", ".db") + "/")
        print(f"Reading data from Parquet path: {full_path}")
        
        # 为了健壮性，这里应该用 try-except
        try:
            dataset = ray.data.read_parquet(full_path)
        except Exception as e:
            print(f"Error reading data from {full_path}: {e}")
            raise

        # --- 3. 动态预处理 ---
        # 将标签和特征分开
        all_cols = dataset.columns()
        feature_cols = [c for c in all_cols if c not in ["is_liked", "user_id", "movieId", "event_timestamp"]]
        label_col = "is_liked"

        # 自动识别数值和类别特征
        numerical_cols = [c for c in feature_cols if dataset.schema().get(c).name in ['int64', 'int32', 'float32', 'float64']]
        categorical_cols = [c for c in feature_cols if c not in numerical_cols]
        
        preprocessors = []
        if numerical_cols:
            preprocessors.append(StandardScaler(columns=numerical_cols))
        if categorical_cols:
            preprocessors.append(OneHotEncoder(columns=categorical_cols))

        if not preprocessors:
            raise ValueError("No features found to preprocess.")

        # 定义一个链式预处理器
        from ray.data.preprocessors import Chain
        preprocessor = Chain(*preprocessors)
        
        # --- 4. 设置 Ray Trainer ---
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2, use_gpu=False), # 根据你的集群资源调整
            datasets={"train": dataset}, # 传入完整数据集
            dataset_config={
                "train": ray.train.DataConfig(
                    # 在这里定义数据分割和预处理
                    split_at_fraction=0.8, # 80% 训练，20% 验证
                    preprocessor=preprocessor,
                    # 将标签和特征列分开
                    transform=lambda df: df.rename(columns={label_col: "label"}).map_batches(
                        lambda batch: {"features": torch.stack([torch.tensor(batch[c]) for c in feature_cols], dim=1), "label": torch.tensor(batch["label"])}
                    )
                )
            },
            train_loop_config={
                "learning_rate": run_parameters.get("learning_rate", 0.01),
                "epochs": run_parameters.get("epochs", 5),
                "batch_size": run_parameters.get("batch_size", 1024),
                "input_size": len(feature_cols) # 动态计算输入维度
            },
        )
        
        # --- 5. 运行训练并获取结果 ---
        result = trainer.fit()
        best_checkpoint = result.best_checkpoints[0][0]
        
        # --- 6. 记录结果到 MLflow ---
        metrics_to_log = {k: v for k, v in result.metrics.items() if isinstance(v, (int, float))}
        mlflow.log_metrics(metrics_to_log)
        
        # 记录模型
        with best_checkpoint.as_directory() as checkpoint_dir:
            model_path = os.path.join(checkpoint_dir, "model.pt")
            mlflow.pytorch.log_model(torch.load(model_path), "model")

        return {"metrics": result.metrics, "model_uri": f"runs:/{run.info.run_id}/model"}

