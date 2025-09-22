# src/training/train.py
# 这是 Ray 训练作业的核心入口，将被 Prefect 任务调用。
import os
import yaml
from typing import Dict

import torch
import mlflow
import pandas as pd

import ray
from ray.air import session
from ray.air.config import ScalingConfig
from ray.train.torch import TorchTrainer, TorchCheckpoint

from src.training.model import SimpleRecommendationModel
from src.training.preprocess import CustomPreprocessor

def train_loop_per_worker(config: Dict):
    """
    在每个 Ray Worker 上执行的训练循环。
    """
    # 从配置中获取参数
    lr = config["lr"]
    epochs = config["epochs"]
    batch_size = config["batch_size"]
    
    # 获取由 Ray Data 准备好的分布式数据集分片
    train_ds = session.get_dataset_shard("train")
    val_ds = session.get_dataset_shard("validation")

    # 实例化模型
    # 注意：输入特征维度需要在上游确定好并传入
    model = SimpleRecommendationModel(
        input_features=config["input_features"],
        config=config["model_architecture"]
    )
    model = ray.train.torch.prepare_model(model)

    criterion = torch.nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # 训练循环
    for epoch in range(epochs):
        # 训练
        model.train()
        for batch in train_ds.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
            # 假设批次是一个字典，包含 "features" 和 "label"
            # 这需要你的 Preprocessor 输出这样的结构
            inputs = batch[[col for col in batch.keys() if col != 'label']]
            labels = batch["label"].unsqueeze(1)

            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
        
        # 验证
        model.eval()
        val_loss = 0
        total_samples = 0
        with torch.no_grad():
            for batch in val_ds.iter_torch_batches(batch_size=batch_size, dtypes=torch.float32):
                inputs = batch[[col for col in batch.keys() if col != 'label']]
                labels = batch["label"].unsqueeze(1)
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                val_loss += loss.item() * len(labels)
                total_samples += len(labels)

        # 使用 session.report() 将指标报告给 Ray AIR/Tune
        session.report(
            {"val_loss": val_loss / total_samples},
            checkpoint=TorchCheckpoint.from_model(model),
        )

def run_ray_training(config_path: str, data_table: str, label_column: str) -> Dict:
    """
    主函数：配置并启动 Ray Trainer。
    这是被 Prefect 任务调用的入口点。
    """
    # 1. 加载配置
    with open(f"{config_path}/base.yaml") as f:
        base_config = yaml.safe_load(f)
    with open(f"{config_path}/model_params.yaml") as f:
        model_params = yaml.safe_load(f)

    # 2. 设置 MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(base_config["mlflow"]["experiment_name"])
    
    # 3. 初始化 Ray
    ray.init(address="auto", ignore_reinit_error=True)

    # 4. 读取数据
    # TODO: 配置 Hive 连接器
    # from ray.data.datasource import HiveDatasource
    # full_ds = ray.data.read_datasource(HiveDatasource(), table=data_table)
    # 临时使用假数据
    full_ds = ray.data.from_pandas(pd.DataFrame({
        "total_orders_7d": [1.0, 2.0, 3.0] * 100,
        "avg_purchase_value_30d": [10.5, 20.2, 5.8] * 100,
        "last_seen_platform": ["ios", "android", "web"] * 100,
        label_column: [0, 1, 0] * 100
    }))
    
    # 5. 定义预处理器并拟合
    feature_cols = [col for col in full_ds.columns() if col != label_column]
    numeric_cols = ["total_orders_7d", "avg_purchase_value_30d"]
    categorical_cols = ["last_seen_platform"]
    
    preprocessor = CustomPreprocessor(numeric_columns=numeric_cols, categorical_columns=categorical_cols)
    
    # 6. 分割数据集
    train_ds, val_ds = full_ds.train_test_split(test_size=0.2)

    # 对训练集拟合预处理器
    preprocessor.fit(train_ds.limit(10000).to_pandas())
    
    # 获取转换后的特征数量，用于模型初始化
    transformed_sample = preprocessor.transform(train_ds.limit(1).to_pandas())
    input_features_dim = transformed_sample.shape[1]

    # 7. 定义 Ray Trainer
    # 在 Ray 2.5+，推荐使用 Ray Train Loop + an MLflowLoggerCallback
    # 这里为了简洁，先展示基本结构
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={
            "lr": model_params["training_loop"]["lr"],
            "epochs": model_params["training_loop"]["epochs"],
            "batch_size": model_params["training_loop"]["batch_size"],
            "model_architecture": model_params["model_architecture"],
            "input_features": input_features_dim
        },
        scaling_config=ScalingConfig(**model_params["scaling_config"]),
        datasets={"train": train_ds, "validation": val_ds},
        preprocessor=preprocessor
    )
    
    # 8. 运行训练
    # 在 MLflow run 的上下文中执行
    with mlflow.start_run() as run:
        result = trainer.fit()
        mlflow_run_id = run.info.run_id
        print(f"MLflow Run ID: {mlflow_run_id}")
        
    # 9. 返回结果
    return {
        "best_checkpoint_path": result.best_checkpoints[0][0].path,
        "metrics": result.metrics,
        "mlflow_run_id": mlflow_run_id
    }