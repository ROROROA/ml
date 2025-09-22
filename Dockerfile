FROM python:3.10-slim

WORKDIR /app
# 复制并安装所有Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安装 prefect-git 用于拉取代码
RUN pip install prefect-git