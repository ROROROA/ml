@echo off
echo 🔄 恢复所有服务的端口转发...
echo.

echo 📊 检查当前端口转发状态...
powershell -ExecutionPolicy Bypass -File list_port_forwards.ps1
echo.

echo 🚀 启动核心服务端口转发...

echo 1️⃣ 启动 Prefect 服务器 (端口 4200)...
start /B kubectl port-forward svc/prefect-server 4200:4200 -n prefect

# prefect-server-service:30200

timeout /t 2 /nobreak > nul

echo 2️⃣ 启动 MLflow 服务 (端口 5000)...
start /B kubectl port-forward svc/mlflow 5000:80 -n default
# mlflow-external-service : 30500

timeout /t 2 /nobreak > nul

echo 3️⃣ 启动 Feast 特征服务 (端口 8888)...
start /B kubectl port-forward svc/feast-feast-demo-online 8888:80 -n default

# feast-ui-external-service : 30808

timeout /t 2 /nobreak > nul

echo 4️⃣ 启动 Ray Dashboard (端口 8266)...
start /B kubectl port-forward svc/ray-kuberay-cluster-head-svc 8266:8265 -n default

timeout /t 3 /nobreak > nul


echo 4️⃣ 启动 MINIO( UI端口 7077)...
start /B  kubectl port-forward svc/minio-console 9001:9001 -n default
# minio-external-service: 9000 & 9001 UI



#For Spark 
@REM 127.0.0.1 minio.default.svc.cluster.local
@REM 127.0.0.1 mlflow-postgres-postgresql.default.svc.cluster.local

#Run
@REM kubectl port-forward svc/minio 9000:9000 -n default
@REM kubectl port-forward svc/mlflow-postgres-postgresql 5432:5432 -n default
@REM submit_connect_server.bat











echo.
echo ✅ 端口转发恢复完成！
echo.
echo 📋 服务访问地址:
echo    🎯 Prefect UI:     http://localhost:4200
echo    📈 MLflow UI:      http://localhost:5000  
echo    🍽️ Feast API文档:  http://localhost:8888/docs
echo    🌐 Feast Web UI:   http://localhost:8889
echo    ⚡ Ray Dashboard:  http://localhost:8266
echo.

echo 🔍 最终端口转发状态:
powershell -ExecutionPolicy Bypass -File list_port_forwards.ps1

pause
