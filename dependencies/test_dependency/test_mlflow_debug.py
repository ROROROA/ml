#!/usr/bin/env python3
"""
MLflow 问题调试脚本
"""
import mlflow
import requests
import sys

def debug_mlflow():
    """调试 MLflow 问题"""
    try:
        tracking_uri = "http://localhost:5001"
        print(f"🔍 调试 MLflow: {tracking_uri}")
        
        # 1. 测试服务可访问性
        response = requests.get(f"{tracking_uri}/health", timeout=10)
        print(f"✅ 服务健康检查: {response.status_code}")
        
        # 2. 测试API访问
        response = requests.get(f"{tracking_uri}/api/2.0/mlflow/experiments/list", timeout=10)
        print(f"✅ API 访问: {response.status_code}")
        if response.status_code == 200:
            experiments = response.json()
            print(f"✅ 实验列表: {experiments}")
        
        # 3. 设置客户端
        mlflow.set_tracking_uri(tracking_uri)
        print(f"✅ 设置 tracking URI: {mlflow.get_tracking_uri()}")
        
        # 4. 获取默认实验信息
        try:
            default_exp = mlflow.get_experiment("0")
            print(f"✅ 默认实验: {default_exp.name}")
        except Exception as e:
            print(f"❌ 获取默认实验失败: {e}")
        
        # 5. 尝试最简单的操作
        try:
            # 直接在 run 中完成所有操作，不引用 run 对象
            with mlflow.start_run():
                mlflow.log_param("simple_test", "value")
                print("✅ 成功记录参数")
                mlflow.log_metric("simple_metric", 1.0)
                print("✅ 成功记录指标")
                # 不获取 run_id，直接结束
            print("✅ Run 完成")
        except Exception as e:
            print(f"❌ Run 操作失败: {e}")
        
        print("🎉 MLflow 基本功能验证完成")
        return True
        
    except Exception as e:
        print(f"❌ MLflow 调试失败: {e}")
        return False

if __name__ == "__main__":
    success = debug_mlflow()
    sys.exit(0 if success else 1)