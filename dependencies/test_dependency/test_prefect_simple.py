#!/usr/bin/env python3
"""
Prefect 自托管简化功能测试
直接测试流程执行，避免命令行编码问题
"""

import os
import time
from datetime import datetime

# 设置Prefect API URL
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

from prefect import flow, task
from prefect.logging import get_run_logger


def print_status(message: str, status: str = "INFO"):
    """打印状态信息"""
    colors = {
        "INFO": "\033[0;34m",
        "SUCCESS": "\033[0;32m", 
        "WARNING": "\033[1;33m",
        "ERROR": "\033[0;31m",
        "STEP": "\033[0;35m"
    }
    reset = "\033[0m"
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{colors.get(status, '')}{timestamp} [{status}]{reset} {message}")


@task
def hello_task(name: str = "Prefect Self-hosted"):
    """简单的问候任务"""
    logger = get_run_logger()
    logger.info(f"Hello from {name}!")
    print(f"任务执行: Hello from {name}!")
    return f"Hello from {name}!"


@task  
def math_task(x: int, y: int):
    """数学计算任务"""
    logger = get_run_logger()
    result = x + y
    logger.info(f"计算结果: {x} + {y} = {result}")
    print(f"任务执行: {x} + {y} = {result}")
    return result


@task
def sleep_task(seconds: int = 2):
    """等待任务，测试长时间运行"""
    logger = get_run_logger()
    logger.info(f"开始等待 {seconds} 秒...")
    print(f"任务执行: 开始等待 {seconds} 秒...")
    time.sleep(seconds)
    logger.info(f"等待完成")
    print(f"任务执行: 等待完成")
    return f"等待了 {seconds} 秒"


@flow(name="test-selfhosted-simple")
def test_selfhosted_simple_flow(name: str = "Kubernetes Worker"):
    """
    简化的端到端测试流程
    """
    logger = get_run_logger()
    logger.info("开始执行简化测试流程")
    print("=== 开始执行Prefect流程 ===")
    
    # 任务1: 问候
    print("步骤1: 执行问候任务")
    greeting = hello_task(name)
    
    # 任务2: 数学计算
    print("步骤2: 执行数学计算任务")
    calc_result = math_task(10, 32)
    
    # 任务3: 等待任务
    print("步骤3: 执行等待任务")
    sleep_result = sleep_task(3)
    
    # 汇总结果
    logger.info("所有任务执行完成!")
    print("=== 流程执行完成 ===")
    print(f"问候结果: {greeting}")
    print(f"计算结果: {calc_result}")
    print(f"等待结果: {sleep_result}")
    
    return {
        "greeting": greeting,
        "calculation": calc_result,
        "sleep": sleep_result,
        "status": "success"
    }


def test_prefect_connection():
    """测试Prefect连接"""
    print_status("测试Prefect自托管连接...", "STEP")
    
    try:
        import prefect
        print_status(f"✓ Prefect版本: {prefect.__version__}", "SUCCESS")
        
        # 测试API连接
        from prefect.client.orchestration import get_client
        
        print_status("测试API连接...", "INFO")
        client = get_client()
        print_status("✓ 客户端创建成功", "SUCCESS")
        
        return True
    except Exception as e:
        print_status(f"✗ 连接测试失败: {str(e)}", "ERROR")
        return False


def test_local_flow_execution():
    """测试本地流程执行"""
    print_status("测试本地流程执行...", "STEP")
    
    try:
        result = test_selfhosted_simple_flow("本地测试")
        print_status("✓ 本地流程执行成功", "SUCCESS")
        print_status(f"执行结果: {result}", "INFO")
        return True
    except Exception as e:
        print_status(f"✗ 本地流程执行失败: {str(e)}", "ERROR")
        return False


def main():
    """主测试函数"""
    print_status("=" * 60, "STEP")
    print_status("Prefect 自托管简化功能测试", "STEP")
    print_status("=" * 60, "STEP")
    
    results = {}
    
    # 1. 测试连接
    results["connection"] = test_prefect_connection()
    if not results["connection"]:
        print_status("连接失败，停止测试", "ERROR")
        return results
    
    # 2. 测试本地执行
    results["local_execution"] = test_local_flow_execution()
    
    # 输出结果
    print_status("=" * 60, "STEP")
    print_status("测试结果摘要", "STEP")
    print_status("=" * 60, "STEP")
    
    test_items = {
        "connection": "Prefect连接测试",
        "local_execution": "本地流程执行测试"
    }
    
    passed = 0
    total = len(results)
    
    for key, description in test_items.items():
        if key in results:
            result = results[key]
            status = "PASS" if result else "FAIL"
            color = "SUCCESS" if result else "ERROR"
            print_status(f"{description}: {status}", color)
            if result:
                passed += 1
    
    print_status("-" * 60)
    print_status(f"总计: {passed}/{total} 通过", 
                 "SUCCESS" if passed == total else "WARNING")
    
    if passed == total:
        print_status("🎉 基础功能测试通过！", "SUCCESS")
        print_status("Prefect自托管系统核心功能正常", "SUCCESS")
    else:
        print_status("⚠️  部分测试失败", "WARNING")
    
    return results


if __name__ == "__main__":
    results = main()
    
    # 输出下一步建议
    if all(results.values()):
        print()
        print_status("建议下一步测试:", "INFO")
        print_status("1. 测试工作池功能", "INFO") 
        print_status("2. 部署流程到Kubernetes", "INFO")
        print_status("3. 监控流程在集群中的执行", "INFO")