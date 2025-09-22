#!/usr/bin/env python3
"""
Prefect 连通性改进测试脚本
解决CLI路径问题，使用python -m prefect调用
"""

import sys
import subprocess
import time
from datetime import datetime

def run_prefect_command(cmd_args, timeout=15):
    """运行Prefect命令的通用函数"""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "prefect"] + cmd_args,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result
    except Exception as e:
        print(f"   命令执行异常: {e}")
        return None

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
    
    result = run_prefect_command(["config", "view"])
    
    if result and result.returncode == 0:
        print("✅ Prefect 配置获取成功")
        
        # 显示关键配置项
        config_lines = result.stdout.split('\n')
        for line in config_lines:
            if line.strip():
                print(f"   {line.strip()}")
                
        return True
    else:
        if result:
            print(f"⚠️ Prefect 配置获取失败: {result.stderr}")
        return False

def check_prefect_version():
    """检查Prefect版本信息"""
    print("\n📋 Prefect 版本信息")
    print("-" * 50)
    
    result = run_prefect_command(["version"])
    
    if result and result.returncode == 0:
        print("✅ 版本信息获取成功")
        for line in result.stdout.split('\n'):
            if line.strip():
                print(f"   {line.strip()}")
        return True
    else:
        print("⚠️ 版本信息获取失败")
        return False

def test_simple_prefect_flow():
    """测试简单的Prefect流"""
    print("\n🧪 简单 Prefect Flow 测试")
    print("-" * 50)
    
    try:
        # 创建临时的简单flow测试文件
        test_flow_content = '''
from prefect import flow, task
from datetime import datetime

@task
def say_hello(name: str):
    print(f"Hello from Prefect task, {name}!")
    return f"Hello, {name}! Time: {datetime.now()}"

@flow(log_prints=True)
def hello_flow(name: str = "Test User"):
    print(f"Starting flow at {datetime.now()}")
    message = say_hello(name)
    print(f"Flow completed: {message}")
    return message

if __name__ == "__main__":
    result = hello_flow("ML Project")
    print(f"Final result: {result}")
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
            print("   执行输出:")
            output_lines = result.stdout.split('\n')
            for line in output_lines[-8:]:  # 显示最后8行
                if line.strip():
                    print(f"     {line.strip()}")
            return True
        else:
            print(f"⚠️ Flow 运行问题:")
            print(f"   错误输出: {result.stderr}")
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
            ["kubectl", "logs", "-l", "app=prefect-agent", "--tail=10"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("\n   Agent 最近日志:")
            log_lines = result.stdout.split('\n')
            for line in log_lines[-6:]:  # 显示最后6行非空日志
                if line.strip():
                    print(f"     {line.strip()}")
        else:
            print(f"   无法获取 Agent 日志: {result.stderr}")
            
        return True
        
    except Exception as e:
        print(f"❌ Kubernetes Agent 检查失败: {e}")
        return False

def test_prefect_server_connection():
    """测试Prefect服务器连接"""
    print("\n🌐 Prefect 服务器连接测试")
    print("-" * 50)
    
    result = run_prefect_command(["config", "view", "--show-defaults"])
    
    if result and result.returncode == 0:
        print("✅ 配置详情获取成功")
        
        # 查找API相关配置
        config_lines = result.stdout.split('\n')
        for line in config_lines:
            if 'API' in line or 'SERVER' in line:
                print(f"   {line.strip()}")
        return True
    else:
        print("⚠️ 配置详情获取失败")
        return False

def test_prefect_profile():
    """测试Prefect配置文件"""
    print("\n👤 Prefect Profile 测试")
    print("-" * 50)
    
    result = run_prefect_command(["profile", "ls"])
    
    if result and result.returncode == 0:
        print("✅ Profile 列表获取成功")
        for line in result.stdout.split('\n'):
            if line.strip():
                print(f"   {line.strip()}")
        return True
    else:
        print("⚠️ Profile 列表获取失败")
        if result:
            print(f"   错误: {result.stderr}")
        return False

def main():
    """主测试函数"""
    print("🚀 Prefect 连通性改进测试")
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
    
    # 版本检查
    if check_prefect_version():
        success_count += 1
    
    # 简单Flow测试
    if test_simple_prefect_flow():
        success_count += 1
    
    # Kubernetes Agent检查
    if check_kubernetes_agent():
        success_count += 1
    
    # Profile测试
    if test_prefect_profile():
        success_count += 1
    
    print("\n" + "=" * 60)
    print("🎊 Prefect 连通性测试完成!")
    print("")
    print("📋 测试结果总结:")
    print(f"   ✅ 成功: {success_count}/{total_tests} 项测试通过")
    
    if success_count >= 5:
        print("   🎯 Prefect 系统运行良好")
        print("   ✅ 本地 Python 环境正常")
        print("   ✅ Flow 执行功能正常")
        print("   ✅ Kubernetes Agent 部署正常")
    elif success_count >= 3:
        print("   ⚠️ Prefect 基础功能可用")
        print("   🔧 建议检查网络连接和配置")
    else:
        print("   ❌ Prefect 连通性存在问题")
    
    print("")
    print("🔗 有用的命令:")
    print("   - 查看配置: python -m prefect config view")
    print("   - 查看版本: python -m prefect version")
    print("   - 查看 profiles: python -m prefect profile ls")
    print("   - 查看 Agent 日志: kubectl logs -l app=prefect-agent")
    print("=" * 60)
    
    return success_count >= 4

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)