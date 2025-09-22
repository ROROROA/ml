#!/usr/bin/env python3
"""
Prefect Kubernetes 部署测试
真正测试流程在Kubernetes上的执行
"""

import os
import time
import subprocess
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


def run_command(cmd, timeout=30):
    """运行命令"""
    print_status(f"执行: {cmd}")
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        if result.stdout.strip():
            print_status(f"输出: {result.stdout.strip()}")
        if result.stderr.strip():
            print_status(f"错误: {result.stderr.strip()}", "WARNING")
        return result
    except subprocess.TimeoutExpired:
        print_status(f"命令超时（{timeout}秒）", "ERROR")
        return None
    except Exception as e:
        print_status(f"命令执行异常: {str(e)}", "ERROR")
        return None


@task
def k8s_hello_task(name: str = "Kubernetes Worker"):
    """在Kubernetes中运行的问候任务"""
    logger = get_run_logger()
    message = f"Hello from {name} running in Kubernetes!"
    logger.info(message)
    print(message)
    
    # 打印一些环境信息
    import platform
    import socket
    
    env_info = {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python_version": platform.python_version()
    }
    
    logger.info(f"环境信息: {env_info}")
    print(f"环境信息: {env_info}")
    
    return message


@task  
def k8s_compute_task(x: int, y: int):
    """在Kubernetes中运行的计算任务"""
    logger = get_run_logger()
    result = x * y + (x + y)
    message = f"Kubernetes计算: {x} * {y} + ({x} + {y}) = {result}"
    logger.info(message)
    print(message)
    return result


@task
def k8s_info_task():
    """获取Kubernetes环境信息"""
    logger = get_run_logger()
    
    try:
        # 尝试获取Pod信息
        import os
        pod_name = os.environ.get('HOSTNAME', 'unknown')
        namespace = os.environ.get('NAMESPACE', 'unknown')
        
        info = {
            "pod_name": pod_name,
            "namespace": namespace,
            "env_vars": {k: v for k, v in os.environ.items() if k.startswith('PREFECT')}
        }
        
        logger.info(f"Kubernetes Pod信息: {info}")
        print(f"Kubernetes Pod信息: {info}")
        
        return info
    except Exception as e:
        logger.error(f"获取K8s信息失败: {str(e)}")
        return {"error": str(e)}


@flow(name="test-k8s-deployment")
def test_k8s_deployment_flow():
    """
    Kubernetes部署测试流程
    """
    logger = get_run_logger()
    logger.info("开始Kubernetes部署测试流程")
    print("=== Kubernetes部署测试开始 ===")
    
    # 任务1: K8s问候
    print("步骤1: Kubernetes问候任务")
    greeting = k8s_hello_task("Prefect Self-hosted K8s Worker")
    
    # 任务2: K8s计算
    print("步骤2: Kubernetes计算任务")
    calc_result = k8s_compute_task(7, 6)
    
    # 任务3: K8s环境信息
    print("步骤3: Kubernetes环境信息")
    k8s_info = k8s_info_task()
    
    result = {
        "greeting": greeting,
        "calculation": calc_result,
        "k8s_info": k8s_info,
        "status": "kubernetes_success"
    }
    
    logger.info("Kubernetes部署测试完成!")
    logger.info(f"测试结果: {result}")
    print("=== Kubernetes部署测试完成 ===")
    print(f"测试结果: {result}")
    
    return result


def test_k8s_deployment():
    """测试Kubernetes部署"""
    print_status("=" * 60, "STEP")
    print_status("Prefect Kubernetes 部署功能测试", "STEP")
    print_status("=" * 60, "STEP")
    
    # 1. 创建部署
    print_status("步骤1: 创建Kubernetes部署", "STEP")
    
    deploy_cmd = '''python -m prefect deploy test_prefect_k8s.py:test_k8s_deployment_flow --name k8s-test-deployment --work-pool kubernetes-pool --image prefecthq/prefect:3-latest'''
    
    result = run_command(deploy_cmd, timeout=60)
    if not result or result.returncode != 0:
        print_status("✗ 部署创建失败", "ERROR")
        return False
    
    print_status("✓ 部署创建成功", "SUCCESS")
    
    # 2. 触发运行
    print_status("步骤2: 触发部署运行", "STEP")
    
    run_cmd = "python -m prefect deployment run test-k8s-deployment/k8s-test-deployment"
    result = run_command(run_cmd, timeout=30)
    if not result or result.returncode != 0:
        print_status("✗ 部署运行失败", "ERROR")
        return False
    
    print_status("✓ 部署运行已触发", "SUCCESS")
    
    # 3. 监控执行
    print_status("步骤3: 监控执行状态", "STEP")
    
    for i in range(12):  # 监控2分钟
        print_status(f"检查运行状态 ({i+1}/12)...")
        
        # 检查最近的流程运行
        ls_cmd = "python -m prefect flow-run ls --limit 3"
        result = run_command(ls_cmd, timeout=15)
        
        if result and result.returncode == 0:
            output = result.stdout
            if "Completed" in output and "test-k8s-deployment" in output:
                print_status("✓ 流程执行完成!", "SUCCESS")
                return True
            elif "Failed" in output and "test-k8s-deployment" in output:
                print_status("✗ 流程执行失败", "ERROR")
                return False
            elif "Running" in output:
                print_status("● 流程正在执行中...", "INFO")
            else:
                print_status("● 等待流程开始...", "INFO")
        
        time.sleep(10)
    
    print_status("⚠️  监控超时，请手动检查", "WARNING")
    return False


def check_k8s_pods():
    """检查Kubernetes Pods"""
    print_status("检查Kubernetes中的执行Pod", "STEP")
    
    # 检查prefect命名空间的Pod
    cmd = "kubectl get pods -n prefect"
    result = run_command(cmd, timeout=15)
    
    if result and result.returncode == 0:
        print_status("✓ Pod状态检查完成", "SUCCESS")
        return True
    else:
        print_status("✗ Pod状态检查失败", "ERROR")
        return False


def get_flow_logs():
    """获取最新的流程日志"""
    print_status("获取流程执行日志", "STEP")
    
    cmd = "python -m prefect flow-run logs --head 30"
    result = run_command(cmd, timeout=20)
    
    if result and result.returncode == 0:
        print_status("✓ 日志获取成功", "SUCCESS")
        return True
    else:
        print_status("✗ 日志获取失败", "ERROR")
        return False


def main():
    """主测试函数"""
    results = {}
    
    # 测试Kubernetes部署
    results["k8s_deployment"] = test_k8s_deployment()
    
    # 检查Pod状态
    results["pod_check"] = check_k8s_pods()
    
    # 获取日志
    results["logs"] = get_flow_logs()
    
    # 输出结果摘要
    print_status("=" * 60, "STEP")
    print_status("Kubernetes部署测试结果", "STEP")
    print_status("=" * 60, "STEP")
    
    test_items = {
        "k8s_deployment": "Kubernetes部署和执行",
        "pod_check": "Pod状态检查",
        "logs": "执行日志获取"
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
    
    if results.get("k8s_deployment", False):
        print_status("🎉 Kubernetes部署功能测试成功!", "SUCCESS")
        print_status("Prefect自托管系统在Kubernetes上完全正常工作!", "SUCCESS")
    else:
        print_status("⚠️  Kubernetes部署功能需要进一步检查", "WARNING")
    
    return results


if __name__ == "__main__":
    # 首先将当前脚本保存到test_prefect_k8s.py以供部署使用
    with open(__file__, 'r', encoding='utf-8') as f:
        content = f.read()
    
    with open('test_prefect_k8s.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    print_status("已创建test_prefect_k8s.py用于部署", "INFO")
    
    # 运行测试
    results = main()
    
    # 清理
    import os
    if os.path.exists('test_prefect_k8s.py'):
        os.remove('test_prefect_k8s.py')
        print_status("已清理临时文件", "INFO")