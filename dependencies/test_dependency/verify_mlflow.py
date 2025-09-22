import mlflow
import os

# 设置 MLflow Tracking Server 的 URI
mlflow_tracking_uri = "http://localhost:5000"
mlflow.set_tracking_uri(mlflow_tracking_uri)

try:
    # 创建或获取一个实验
    experiment_name = "MLflow Connectivity Test"
    # 使用 get_experiment_by_name 来避免重复创建
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"Experiment '{experiment_name}' created with ID: {experiment_id}")
    else:
        experiment_id = experiment.experiment_id
        print(f"Experiment '{experiment_name}' already exists with ID: {experiment_id}")

    print("\nMLflow connectivity test PASSED!")
    print(f"Successfully connected to MLflow at '{mlflow_tracking_uri}' and verified experiment creation.")

except Exception as e:
    print(f"\nMLflow connectivity test FAILED.")
    print(f"Could not connect to MLflow Tracking Server at '{mlflow_tracking_uri}'.")
    print("Please ensure that the MLflow service is running in your Kubernetes cluster and that port-forwarding is set up correctly.")
    print(f"Error details: {e}")