# 1. 从你现在可以正常工作的 Spark 基础镜像开始
FROM docker.io/library/spark:4.0.0

# 2. 切换到 root 用户来执行安装和权限修改
USER root

# 3. 安装构建依赖，并使用 NodeSource 脚本安装 Node.js v20
RUN apt-get update && \
    apt-get install -y curl python3-pip && \
    curl -sL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. 使用 pip 安装 JupyterLab 和 findspark (系统级安装)
RUN pip install jupyterlab findspark

# 5. 提前构建 JupyterLab 的前端资源
RUN jupyter lab build

# 6. --- 这里是修正的部分 ---
#    先创建 /home/spark 目录，然后再更改它的所有权
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# 7. 把默认用户切换回 spark，并设置工作目录
USER spark
WORKDIR /home/spark