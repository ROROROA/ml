#!/usr/bin/env python3
"""
简化的 MLflow 连通性测试
"""
import mlflow
import requests
import sys

def simple_mlflow_test():
    """简单的 MLflow 测试"""
    try:
        tracking_uri = "http://localhost:5001"
        print(f"🔍 测试 MLflow 连通性: {tracking_uri}")
        
        # 测试 HTTP 连通性
        response = requests.get(f"{tracking_uri}/health", timeout=10)
        print(f"✅ MLflow 服务健康检查: {response.status_code}")
        
        # 设置 MLflow
        mlflow.set_tracking_uri(tracking_uri)
        
        # 简单测试：获取默认实验
        experiment = mlflow.get_experiment("0")  # 默认实验ID是0
        print(f"✅ 成功获取默认实验: {experiment.name}")
        
        # 创建一个简单的 run
        with mlflow.start_run():
            mlflow.log_param("test_param", "simple_test")
            mlflow.log_metric("test_metric", 1.0)
            print("✅ 成功创建 run 并记录参数和指标")
        
        print("🎉 MLflow 连通性测试成功!")
        return True
        
    except Exception as e:
        print(f"❌ MLflow 测试失败: {e}")
        return False

if __name__ == "__main__":
    success = simple_mlflow_test()
    sys.exit(0 if success else 1)