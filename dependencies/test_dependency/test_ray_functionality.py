#!/usr/bin/env python3
"""
Ray 功能测试脚本
验证Ray集群的基本功能
"""

import time
import sys
from datetime import datetime

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

def test_ray_import():
    """测试Ray导入"""
    print_status("测试Ray模块导入...", "STEP")
    try:
        import ray
        print_status(f"✓ Ray版本: {ray.__version__}", "SUCCESS")
        return True
    except ImportError as e:
        print_status(f"✗ Ray导入失败: {str(e)}", "ERROR")
        print_status("请安装Ray: pip install ray", "INFO")
        return False

def test_ray_connection():
    """测试Ray集群连接"""
    print_status("测试Ray集群连接...", "STEP")
    try:
        import ray
        
        # 连接到本地Ray集群
        ray.init(address="ray://localhost:10001", ignore_reinit_error=True)
        
        print_status("✓ 成功连接到Ray集群", "SUCCESS")
        
        # 获取集群信息
        cluster_resources = ray.cluster_resources()
        print_status(f"集群资源: {cluster_resources}", "INFO")
        
        return True
    except Exception as e:
        print_status(f"✗ Ray集群连接失败: {str(e)}", "ERROR")
        return False

def test_basic_task():
    """测试基本任务执行"""
    print_status("测试Ray任务执行...", "STEP")
    try:
        import ray
        
        @ray.remote
        def simple_task(x):
            return x * x
        
        # 执行简单任务
        future = simple_task.remote(4)
        result = ray.get(future)
        
        if result == 16:
            print_status(f"✓ 任务执行成功: 4^2 = {result}", "SUCCESS")
            return True
        else:
            print_status(f"✗ 任务结果错误: 期望16，得到{result}", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"✗ 任务执行失败: {str(e)}", "ERROR")
        return False

def test_parallel_tasks():
    """测试并行任务执行"""
    print_status("测试Ray并行任务...", "STEP")
    try:
        import ray
        
        @ray.remote
        def compute_task(x):
            time.sleep(0.1)  # 模拟计算时间
            return x * 2
        
        # 并行执行多个任务
        start_time = time.time()
        futures = [compute_task.remote(i) for i in range(10)]
        results = ray.get(futures)
        end_time = time.time()
        
        expected = [i * 2 for i in range(10)]
        if results == expected:
            execution_time = end_time - start_time
            print_status(f"✓ 并行任务执行成功，耗时: {execution_time:.2f}秒", "SUCCESS")
            print_status(f"结果: {results}", "INFO")
            return True
        else:
            print_status(f"✗ 并行任务结果错误", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"✗ 并行任务执行失败: {str(e)}", "ERROR")
        return False

def test_actor_pattern():
    """测试Actor模式"""
    print_status("测试Ray Actor模式...", "STEP")
    try:
        import ray
        
        @ray.remote
        class Counter:
            def __init__(self):
                self.value = 0
            
            def increment(self):
                self.value += 1
                return self.value
            
            def get_value(self):
                return self.value
        
        # 创建Actor实例
        counter = Counter.remote()
        
        # 执行操作
        counter.increment.remote()
        counter.increment.remote()
        value = ray.get(counter.get_value.remote())
        
        if value == 2:
            print_status(f"✓ Actor模式测试成功: 计数器值 = {value}", "SUCCESS")
            return True
        else:
            print_status(f"✗ Actor模式测试失败: 期望2，得到{value}", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"✗ Actor模式测试失败: {str(e)}", "ERROR")
        return False

def cleanup_ray():
    """清理Ray连接"""
    try:
        import ray
        ray.shutdown()
        print_status("✓ Ray连接已清理", "INFO")
    except Exception as e:
        print_status(f"清理Ray连接时出错: {str(e)}", "WARNING")

def main():
    """主测试函数"""
    print_status("=" * 60, "STEP")
    print_status("Ray 集群功能测试", "STEP")
    print_status("=" * 60, "STEP")
    
    results = {}
    
    try:
        # 1. 测试Ray导入
        results["import"] = test_ray_import()
        if not results["import"]:
            print_status("Ray模块不可用，停止测试", "ERROR")
            return results
        
        # 2. 测试集群连接
        results["connection"] = test_ray_connection()
        if not results["connection"]:
            print_status("无法连接Ray集群，跳过后续测试", "WARNING")
            return results
        
        # 3. 测试基本任务
        results["basic_task"] = test_basic_task()
        
        # 4. 测试并行任务
        results["parallel_tasks"] = test_parallel_tasks()
        
        # 5. 测试Actor模式
        results["actor_pattern"] = test_actor_pattern()
        
    except Exception as e:
        print_status(f"测试过程中发生错误: {str(e)}", "ERROR")
        results["error"] = str(e)
    
    finally:
        cleanup_ray()
    
    # 输出结果摘要
    print_status("=" * 60, "STEP")
    print_status("Ray测试结果摘要", "STEP")
    print_status("=" * 60, "STEP")
    
    test_items = {
        "import": "Ray模块导入",
        "connection": "集群连接",
        "basic_task": "基本任务执行",
        "parallel_tasks": "并行任务执行",
        "actor_pattern": "Actor模式"
    }
    
    passed = 0
    total = len([k for k in results.keys() if k != "error"])
    
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
        print_status("🎉 Ray集群功能完全正常！", "SUCCESS")
    else:
        print_status("⚠️  部分功能测试失败", "WARNING")
    
    if "error" in results:
        print_status(f"错误信息: {results['error']}", "ERROR")
    
    return results

if __name__ == "__main__":
    results = main()
    
    # 根据结果设置退出码
    all_passed = all(v for k, v in results.items() if k != "error" and isinstance(v, bool))
    sys.exit(0 if all_passed else 1)