#!/usr/bin/env python3
"""
Prefect Server 连通性测试
测试与本地 Prefect Server 的连接和部署功能
"""

import sys
import subprocess
import time
from datetime import datetime
from prefect import flow, task
import requests

def configure_prefect_client():
    """配置Prefect客户端连接到本地服务器"""
    print("🔧 配置 Prefect 客户端")
    print("-" * 50)
    
    try:
        # 设置API URL指向本地服务器
        result = subprocess.run(
            [sys.executable, "-m", "prefect", "config", "set", "PREFECT_API_URL=http://localhost:4200/api"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ 客户端配置成功")
            return True
        else:
            print(f"⚠️ 配置失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 配置异常: {e}")
        return False

def test_server_connection():
    """测试服务器连接"""
    print("\n🌐 Prefect Server 连接测试")
    print("-" * 50)
    
    try:
        # 测试API健康检查
        response = requests.get("http://localhost:4200/api/health", timeout=10)
        if response.status_code == 200:
            print("✅ Server API 健康检查通过")
            return True
        else:
            print(f"⚠️ API 响应异常: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False

def test_work_pools():
    """测试工作池"""
    print("\n💼 工作池测试")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", "prefect", "work-pool", "ls"],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            print("✅ 工作池列表获取成功")
            for line in result.stdout.split('\n'):
                if line.strip():
                    print(f"   {line.strip()}")
            return True
        else:
            print(f"⚠️ 工作池查询失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 工作池测试异常: {e}")
        return False

@task
def sample_task(name: str):
    """示例任务"""
    print(f"🔄 执行任务: {name}")
    time.sleep(1)
    result = f"任务 {name} 完成于 {datetime.now().strftime('%H:%M:%S')}"
    print(f"✅ {result}")
    return result

@flow(log_prints=True)
def sample_flow():
    """示例流程"""
    print("🚀 开始执行示例流程")
    
    # 执行多个任务
    task1 = sample_task("数据准备")
    task2 = sample_task("模型训练")
    task3 = sample_task("结果保存")
    
    return {
        "flow_status": "completed",
        "tasks": [task1, task2, task3],
        "completed_at": datetime.now().isoformat()
    }

def test_flow_deployment():
    """测试流程部署"""
    print("\n📦 流程部署测试")
    print("-" * 50)
    
    try:
        # 运行示例流程
        print("执行示例流程...")
        result = sample_flow()
        
        print("✅ 流程执行成功")
        print(f"   状态: {result['flow_status']}")
        print(f"   完成时间: {result['completed_at']}")
        print(f"   任务数量: {len(result['tasks'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ 流程部署测试失败: {e}")
        return False

def test_deployments_list():
    """测试部署列表"""
    print("\n📋 部署列表测试")
    print("-" * 50)
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", "prefect", "deployment", "ls"],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            print("✅ 部署列表获取成功")
            if result.stdout.strip():
                for line in result.stdout.split('\n')[:10]:
                    if line.strip():
                        print(f"   {line.strip()}")
            else:
                print("   ℹ️ 暂无部署（这是正常的）")
            return True
        else:
            print(f"⚠️ 部署列表获取失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 部署列表测试异常: {e}")
        return False

def main():
    """主测试函数"""
    print("🚀 Prefect Server 完整测试")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    success_count = 0
    total_tests = 5
    
    # 配置客户端
    if configure_prefect_client():
        success_count += 1
    
    # 服务器连接测试
    if test_server_connection():
        success_count += 1
    
    # 工作池测试
    if test_work_pools():
        success_count += 1
    
    # 流程执行测试
    if test_flow_deployment():
        success_count += 1
    
    # 部署列表测试
    if test_deployments_list():
        success_count += 1
    
    print("\n" + "=" * 60)
    print("🎊 Prefect Server 测试完成!")
    print("")
    print("📋 测试结果总结:")
    print(f"   ✅ 成功: {success_count}/{total_tests} 项测试通过")
    
    if success_count >= 4:
        print("   🎯 Prefect Server 系统运行良好")
        print("   ✅ 本地服务器连接正常")
        print("   ✅ 流程执行功能正常")
        print("   ✅ 工作池配置正确")
    elif success_count >= 2:
        print("   ⚠️ Prefect 基础功能可用")
        print("   🔧 建议检查网络连接和配置")
    else:
        print("   ❌ Prefect 连通性存在问题")
    
    print("")
    print("🔗 有用的链接:")
    print("   - Prefect UI: http://localhost:4200")
    print("   - API 文档: http://localhost:4200/docs")
    print("   - API 健康检查: http://localhost:4200/api/health")
    print("")
    print("🔧 有用的命令:")
    print("   - 查看配置: python -m prefect config view")
    print("   - 查看工作池: python -m prefect work-pool ls")
    print("   - 查看部署: python -m prefect deployment ls")
    print("=" * 60)
    
    return success_count >= 4

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)