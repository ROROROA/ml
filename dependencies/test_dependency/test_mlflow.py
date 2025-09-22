#!/usr/bin/env python3
"""
MLflow 连通性测试脚本
"""
import mlflow
import requests
import sys

def test_mlflow_connectivity():
    """测试 MLflow 连通性"""
    try:
        # 设置 MLflow tracking URI
        mlflow_host = "localhost"
        mlflow_port = "5001"  # Port-forward
        tracking_uri = f"http://{mlflow_host}:{mlflow_port}"
        
        print(f"🔍 测试 MLflow 连通性...")
        print(f"📍 MLflow Tracking URI: {tracking_uri}")
        
        # 先测试 HTTP 连通性
        try:
            response = requests.get(f"{tracking_uri}/health", timeout=10)
            print(f"✅ MLflow 服务健康检查通过: {response.status_code}")
        except Exception as e:
            print(f"❌ MLflow 服务健康检查失败: {e}")
            print("💡 尝试端口转发: kubectl port-forward svc/mlflow-nodeport 30500:5000")
            return False
        
        # 设置 MLflow 客户端
        mlflow.set_tracking_uri(tracking_uri)
        
        # 直接使用默认实验
        print("✅ 使用默认实验")
        
        # 开始一个 MLflow run
        with mlflow.start_run() as run:
            print(f"✅ 开始 MLflow run: {run.info.run_id}")
            
            # 记录参数
            mlflow.log_param("test_param", "connectivity_test")
            print("✅ 记录参数成功")
            
            # 记录指标
            mlflow.log_metric("test_metric", 0.85)
            print("✅ 记录指标成功")
            
            print(f"🎉 MLflow 连通性测试完全成功!")
            print(f"📊 Run ID: {run.info.run_id}")
            print(f"🌐 访问 MLflow UI: {tracking_uri}")
            
        return True
        
    except Exception as e:
        print(f"❌ MLflow 连通性测试失败: {e}")
        return False

if __name__ == "__main__":
    success = test_mlflow_connectivity()
    sys.exit(0 if success else 1)