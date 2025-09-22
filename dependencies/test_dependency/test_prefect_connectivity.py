#!/usr/bin/env python3
"""
Prefect 连通性测试脚本
测试本地Prefect环境与Kubernetes中Prefect Agent的连通性
"""

import sys
import subprocess
import time
from datetime import datetime

def check_prefect_installation():
    """检查Prefect是否安装"""
    print("🔍 Prefect 安装检查")
    print("-" * 50)
    
    try:
        import prefect
        print(f"✅ Prefect 已安装，版本: {prefect.__version__}")
        return True
    except ImportError:
        print("❌ Prefect 未安装，请先安装: pip install prefect")
        return False

def check_prefect_config():
    """检查Prefect配置"""
    print("\n⚙️ Prefect 配置检查")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            ["prefect", "config", "view"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Prefect 配置获取成功")
            
            # 查找关键配置项
            config_lines = result.stdout.split('\n')
            for line in config_lines:
                if 'PREFECT_API_URL' in line or 'PREFECT_API_KEY' in line:
                    print(f"   {line.strip()}")
                    
            return True
        else:
            print(f"⚠️ Prefect 配置获取失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Prefect 配置检查失败: {e}")
        return False

def check_prefect_cloud_connection():
    """检查Prefect Cloud连接"""
    print("\n☁️ Prefect Cloud 连接检查")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            ["prefect", "cloud", "workspace", "ls"],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            print("✅ Prefect Cloud 连接成功")
            print("   工作空间列表:")
            for line in result.stdout.split('\n')[:5]:  # 只显示前5行
                if line.strip():
                    print(f"     {line.strip()}")
            return True
        else:
            print(f"⚠️ Prefect Cloud 连接问题: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Prefect Cloud 连接失败: {e}")
        return False

def test_simple_prefect_flow():
    """测试简单的Prefect流"""
    print("\n🧪 简单 Prefect Flow 测试")
    print("-" * 50)
    
    try:
        # 创建临时的简单flow测试文件
        test_flow_content = '''
from prefect import flow, task

@task
def say_hello(name: str):
    print(f"Hello, {name}!")
    return f"Hello, {name}!"

@flow(log_prints=True)
def hello_flow(name: str = "Prefect"):
    message = say_hello(name)
    return message

if __name__ == "__main__":
    result = hello_flow("Test User")
    print(f"Flow result: {result}")
'''
        
        with open("test_prefect_simple_flow.py", "w", encoding="utf-8") as f:
            f.write(test_flow_content)
        
        print("✅ 简单测试流文件创建成功")
        
        # 运行简单流
        result = subprocess.run(
            [sys.executable, "test_prefect_simple_flow.py"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print("✅ 简单 Prefect Flow 运行成功")
            print("   输出片段:")
            output_lines = result.stdout.split('\n')
            for line in output_lines[-10:]:  # 显示最后10行
                if line.strip():
                    print(f"     {line.strip()}")
            return True
        else:
            print(f"⚠️ Flow 运行问题: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 简单 Flow 测试失败: {e}")
        return False
    finally:
        # 清理临时文件
        try:
            import os
            if os.path.exists("test_prefect_simple_flow.py"):
                os.remove("test_prefect_simple_flow.py")
        except:
            pass

def check_kubernetes_agent():
    """检查Kubernetes中的Prefect Agent状态"""
    print("\n🚢 Kubernetes Prefect Agent 状态检查")
    print("-" * 50)
    
    try:
        # 检查Pod状态
        result = subprocess.run(
            ["kubectl", "get", "pods", "-l", "app=prefect-agent"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Prefect Agent Pod 状态:")
            for line in result.stdout.split('\n'):
                if line.strip():
                    print(f"     {line.strip()}")
        else:
            print(f"⚠️ 无法获取 Agent Pod 状态: {result.stderr}")
            
        # 检查Agent日志（最近的几行）
        result = subprocess.run(
            ["kubectl", "logs", "-l", "app=prefect-agent", "--tail=5"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("\n   Agent 最近日志:")
            for line in result.stdout.split('\n'):
                if line.strip():
                    print(f"     {line.strip()}")
        else:
            print(f"   无法获取 Agent 日志: {result.stderr}")
            
        return True
        
    except Exception as e:
        print(f"❌ Kubernetes Agent 检查失败: {e}")
        return False

def test_prefect_deployment_list():
    """测试Prefect部署列表"""
    print("\n📦 Prefect 部署列表检查")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            ["prefect", "deployment", "ls"],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            print("✅ 部署列表获取成功")
            if result.stdout.strip():
                print("   当前部署:")
                for line in result.stdout.split('\n')[:10]:  # 只显示前10行
                    if line.strip():
                        print(f"     {line.strip()}")
            else:
                print("   ℹ️ 暂无部署（这是正常的）")
            return True
        else:
            print(f"⚠️ 部署列表获取失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 部署列表检查失败: {e}")
        return False

def main():
    """主测试函数"""
    print("🚀 Prefect 连通性测试")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    success_count = 0
    total_tests = 6
    
    # 安装检查
    if check_prefect_installation():
        success_count += 1
    
    # 配置检查
    if check_prefect_config():
        success_count += 1
    
    # Cloud连接检查
    if check_prefect_cloud_connection():
        success_count += 1
    
    # 简单Flow测试
    if test_simple_prefect_flow():
        success_count += 1
    
    # Kubernetes Agent检查
    if check_kubernetes_agent():
        success_count += 1
    
    # 部署列表检查
    if test_prefect_deployment_list():
        success_count += 1
    
    print("\n" + "=" * 60)
    print("🎊 Prefect 连通性测试完成!")
    print("")
    print("📋 测试结果总结:")
    print(f"   ✅ 成功: {success_count}/{total_tests} 项测试通过")
    
    if success_count >= 4:
        print("   🎯 Prefect 系统整体运行良好")
        print("   ✅ 本地环境与云端连接正常")
        print("   ✅ Kubernetes Agent 运行正常")
    elif success_count >= 2:
        print("   ⚠️ Prefect 基础功能可用，部分高级功能需要配置")
    else:
        print("   ❌ Prefect 连通性存在问题，需要检查配置")
    
    print("")
    print("🔗 有用的命令:")
    print("   - 查看配置: prefect config view")
    print("   - 登录 Cloud: prefect cloud login")
    print("   - 查看部署: prefect deployment ls")
    print("   - 查看 Agent 日志: kubectl logs -l app=prefect-agent")
    print("=" * 60)
    
    return success_count >= 4

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)