#!/usr/bin/env python3
"""
简单的Kubernetes测试流程
"""

import os
import time
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

from prefect import flow, task
from prefect.logging import get_run_logger


@task
def k8s_test_task(name: str = "Kubernetes"):
    """Kubernetes测试任务"""
    logger = get_run_logger()
    
    # 获取环境信息
    import platform
    import socket
    
    message = f"Hello from {name}!"
    env_info = f"Running on {socket.gethostname()} with Python {platform.python_version()}"
    
    logger.info(message)
    logger.info(env_info)
    
    print(f"[TASK] {message}")
    print(f"[TASK] {env_info}")
    
    return {"message": message, "env": env_info}


@flow(name="simple-k8s-test")
def simple_k8s_test():
    """简单的Kubernetes测试流程"""
    logger = get_run_logger()
    logger.info("开始Kubernetes测试")
    
    result = k8s_test_task("Prefect Self-hosted Worker")
    
    logger.info("Kubernetes测试完成")
    logger.info(f"结果: {result}")
    
    return result


if __name__ == "__main__":
    # 本地测试
    print("=== 本地测试 ===")
    result = simple_k8s_test()
    print(f"本地测试结果: {result}")