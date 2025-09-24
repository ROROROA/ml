# FROM python:3.12-slim

# WORKDIR /app
# # 复制并安装所有Python依赖
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # 安装 prefect-git 用于拉取代码
# RUN pip install "prefect[github]"
# RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
# RUN pip install prefect-shell
# RUN pip install grpcio==1.71.0 
# RUN pip install grpcio-status==1.71.0 

# ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
# ENV PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

# RUN pip install torch
# RUN pip install feast[redis]


FROM my-ml-base-image:latest
RUN pip install redis==4.6.0
RUN pip install psycopg-binary==3.2.10
RUN pip install psycopg==3.2.10
RUN pip install prefect-ray==0.4.5
RUN pip install s3fs==2023.9.2


# docker build . -t my-ml-base-image:latest




