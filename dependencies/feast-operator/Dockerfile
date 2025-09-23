FROM quay.io/feastdev/feature-server:0.53.0

RUN pip install pyspark==4.0.0


USER root
RUN yum update -y && \
    yum install -y java-17-openjdk-devel && \
    yum clean all
USER 1001

# 设置 JAVA_HOME 环境变量为 Java 17 的路径
# 注意: 路径已更新为 17
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk"

