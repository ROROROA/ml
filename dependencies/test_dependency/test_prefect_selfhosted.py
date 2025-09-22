#!/usr/bin/env python3
"""
Prefect 自托管连接测试和验证脚本
用于验证Prefect服务器和Worker的连接状态
"""

import asyncio
import os
import sys
import time
import httpx
import yaml
from typing import Dict, Any, Optional
from datetime import datetime


class PrefectSelfHostedTester:
    """Prefect自托管测试器"""
    
    def __init__(self, 
                 server_url: str = "http://localhost:4200",
                 api_url: Optional[str] = None):
        self.server_url = server_url
        self.api_url = api_url or f"{server_url}/api"
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    def print_status(self, message: str, status: str = "INFO"):
        """打印状态信息"""
        colors = {
            "INFO": "\033[0;34m",
            "SUCCESS": "\033[0;32m", 
            "WARNING": "\033[1;33m",
            "ERROR": "\033[0;31m"
        }
        reset = "\033[0m"
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{colors.get(status, '')}{timestamp} [{status}]{reset} {message}")
    
    async def test_server_health(self) -> bool:
        """测试服务器健康状态"""
        self.print_status("测试Prefect服务器健康状态...")
        
        try:
            response = await self.client.get(f"{self.api_url}/health")
            
            if response.status_code == 200:
                self.print_status("✓ 服务器健康检查通过", "SUCCESS")
                return True
            else:
                self.print_status(f"✗ 服务器健康检查失败: HTTP {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"✗ 服务器连接失败: {str(e)}", "ERROR")
            return False
    
    async def test_api_version(self) -> bool:
        """测试API版本信息"""
        self.print_status("获取API版本信息...")
        
        try:
            response = await self.client.get(f"{self.api_url}/version")
            
            if response.status_code == 200:
                version_info = response.json()
                self.print_status(f"✓ API版本: {version_info.get('version', 'Unknown')}", "SUCCESS")
                return True
            else:
                self.print_status(f"✗ 获取版本信息失败: HTTP {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"✗ 版本信息请求失败: {str(e)}", "ERROR")
            return False
    
    async def test_work_pools(self) -> bool:
        """测试工作池连接"""
        self.print_status("检查工作池状态...")
        
        try:
            response = await self.client.get(f"{self.api_url}/work_pools/")
            
            if response.status_code == 200:
                pools = response.json()
                self.print_status(f"✓ 发现 {len(pools)} 个工作池", "SUCCESS")
                
                for pool in pools:
                    pool_name = pool.get('name', 'Unknown')
                    pool_type = pool.get('type', 'Unknown')
                    is_paused = pool.get('is_paused', False)
                    status = "暂停" if is_paused else "活跃"
                    self.print_status(f"  - {pool_name} ({pool_type}): {status}")
                
                return True
            else:
                self.print_status(f"✗ 获取工作池失败: HTTP {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"✗ 工作池请求失败: {str(e)}", "ERROR")
            return False
    
    async def test_flows(self) -> bool:
        """测试流程列表"""
        self.print_status("检查已部署的流程...")
        
        try:
            response = await self.client.get(f"{self.api_url}/flows/")
            
            if response.status_code == 200:
                flows = response.json()
                self.print_status(f"✓ 发现 {len(flows)} 个流程", "SUCCESS")
                
                if flows:
                    for flow in flows[:5]:  # 显示前5个
                        flow_name = flow.get('name', 'Unknown')
                        self.print_status(f"  - {flow_name}")
                
                return True
            else:
                self.print_status(f"✗ 获取流程列表失败: HTTP {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"✗ 流程列表请求失败: {str(e)}", "ERROR")
            return False
    
    async def create_test_flow(self) -> bool:
        """创建测试流程"""
        self.print_status("创建测试流程...")
        
        test_flow_code = '''
from prefect import flow, task

@task
def hello_task():
    return "Hello from Prefect self-hosted!"

@flow
def test_flow():
    message = hello_task()
    print(message)
    return message

if __name__ == "__main__":
    test_flow()
'''
        
        try:
            # 保存测试流程文件
            with open("test_selfhosted_flow.py", "w", encoding="utf-8") as f:
                f.write(test_flow_code)
            
            self.print_status("✓ 测试流程文件已创建: test_selfhosted_flow.py", "SUCCESS")
            
            # 提示如何部署
            self.print_status("要部署此流程，请运行:")
            self.print_status(f"  export PREFECT_API_URL={self.api_url}")
            self.print_status("  python test_selfhosted_flow.py")
            
            return True
            
        except Exception as e:
            self.print_status(f"✗ 创建测试流程失败: {str(e)}", "ERROR")
            return False
    
    async def test_ui_access(self) -> bool:
        """测试UI访问"""
        self.print_status("测试Prefect UI访问...")
        
        try:
            response = await self.client.get(self.server_url)
            
            if response.status_code == 200:
                self.print_status(f"✓ UI界面可访问: {self.server_url}", "SUCCESS")
                return True
            else:
                self.print_status(f"✗ UI访问失败: HTTP {response.status_code}", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"✗ UI连接失败: {str(e)}", "ERROR")
            return False
    
    async def run_comprehensive_test(self) -> Dict[str, bool]:
        """运行综合测试"""
        self.print_status("=" * 50)
        self.print_status("Prefect 自托管连接测试开始", "INFO")
        self.print_status("=" * 50)
        
        tests = {
            "服务器健康检查": self.test_server_health,
            "API版本信息": self.test_api_version,
            "UI界面访问": self.test_ui_access,
            "工作池状态": self.test_work_pools,
            "流程列表": self.test_flows,
            "创建测试流程": self.create_test_flow,
        }
        
        results = {}
        
        for test_name, test_func in tests.items():
            try:
                result = await test_func()
                results[test_name] = result
                time.sleep(1)  # 间隔1秒
            except Exception as e:
                self.print_status(f"✗ {test_name} 执行出错: {str(e)}", "ERROR")
                results[test_name] = False
        
        # 输出测试结果摘要
        self.print_status("=" * 50)
        self.print_status("测试结果摘要", "INFO")
        self.print_status("=" * 50)
        
        passed = 0
        total = len(results)
        
        for test_name, result in results.items():
            status = "PASS" if result else "FAIL"
            color = "SUCCESS" if result else "ERROR"
            self.print_status(f"{test_name}: {status}", color)
            if result:
                passed += 1
        
        self.print_status("-" * 50)
        self.print_status(f"总计: {passed}/{total} 通过", 
                         "SUCCESS" if passed == total else "WARNING")
        
        if passed == total:
            self.print_status("🎉 所有测试通过！Prefect自托管服务运行正常", "SUCCESS")
        else:
            self.print_status("⚠️  部分测试失败，请检查服务配置", "WARNING")
        
        return results


def print_usage():
    """打印使用说明"""
    print("""
Prefect 自托管连接测试工具

使用方法:
    python test_prefect_selfhosted.py [选项]

选项:
    --server-url URL    Prefect服务器地址 (默认: http://localhost:4200)
    --api-url URL      Prefect API地址 (默认: 服务器地址/api)
    --help             显示此帮助信息

示例:
    # 使用默认地址测试
    python test_prefect_selfhosted.py
    
    # 使用自定义地址测试
    python test_prefect_selfhosted.py --server-url http://prefect.example.com
    
    # 指定不同的API地址
    python test_prefect_selfhosted.py --server-url http://localhost:4200 --api-url http://localhost:4200/api
""")


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect自托管连接测试")
    parser.add_argument("--server-url", default="http://localhost:4200",
                       help="Prefect服务器地址")
    parser.add_argument("--api-url", default=None,
                       help="Prefect API地址")
    
    args = parser.parse_args()
    
    if "--help" in sys.argv:
        print_usage()
        return
    
    # 检查依赖
    try:
        import httpx
    except ImportError:
        print("错误: 缺少依赖 httpx")
        print("请安装: pip install httpx")
        sys.exit(1)
    
    async with PrefectSelfHostedTester(args.server_url, args.api_url) as tester:
        results = await tester.run_comprehensive_test()
        
        # 根据结果设置退出码
        if all(results.values()):
            sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())