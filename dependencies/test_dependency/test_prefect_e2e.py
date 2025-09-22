#!/usr/bin/env python3
"""
Prefect 自托管端到端功能测试
真正测试流程的创建、部署和在Kubernetes上的执行
"""

import asyncio
import os
import sys
import time
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
import httpx


class PrefectE2ETester:
    """Prefect端到端功能测试器"""
    
    def __init__(self, 
                 server_url: str = "http://localhost:4200",
                 api_url: str = None):
        self.server_url = server_url
        self.api_url = api_url or f"{server_url}/api"
        self.work_pool_name = "kubernetes-pool"
        self.test_flow_name = "test-selfhosted-flow"
        
    def print_status(self, message: str, status: str = "INFO"):
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
    
    def run_command(self, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
        """运行命令并返回结果"""
        self.print_status(f"执行命令: {cmd}")
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True,
                check=check
            )
            if result.stdout:
                self.print_status(f"输出: {result.stdout.strip()}")
            return result
        except subprocess.CalledProcessError as e:
            self.print_status(f"命令执行失败: {e}", "ERROR")
            if e.stdout:
                self.print_status(f"stdout: {e.stdout}", "ERROR")
            if e.stderr:
                self.print_status(f"stderr: {e.stderr}", "ERROR")
            raise
    
    def setup_prefect_client(self):
        """配置Prefect客户端"""
        self.print_status("配置Prefect客户端...")
        
        # 设置API URL
        os.environ["PREFECT_API_URL"] = self.api_url
        self.print_status(f"设置 PREFECT_API_URL={self.api_url}")
        
        # 验证连接
        try:
            result = self.run_command("python -m prefect config view")
            self.print_status("✓ Prefect客户端配置成功", "SUCCESS")
            return True
        except Exception as e:
            self.print_status(f"✗ Prefect客户端配置失败: {str(e)}", "ERROR")
            return False
    
    def create_test_flow(self) -> str:
        """创建测试流程文件"""
        self.print_status("创建测试流程文件...")
        
        flow_content = '''
import time
from prefect import flow, task
from prefect.logging import get_run_logger

@task
def hello_task(name: str = "Prefect Self-hosted"):
    """简单的问候任务"""
    logger = get_run_logger()
    logger.info(f"Hello from {name}!")
    return f"Hello from {name}!"

@task  
def math_task(x: int, y: int):
    """数学计算任务"""
    logger = get_run_logger()
    result = x + y
    logger.info(f"计算结果: {x} + {y} = {result}")
    return result

@task
def sleep_task(seconds: int = 2):
    """等待任务，测试长时间运行"""
    logger = get_run_logger()
    logger.info(f"开始等待 {seconds} 秒...")
    time.sleep(seconds)
    logger.info(f"等待完成")
    return f"等待了 {seconds} 秒"

@flow(name="test-selfhosted-flow")
def test_selfhosted_flow(name: str = "Kubernetes Worker"):
    """
    端到端测试流程
    测试任务执行、日志记录、参数传递等功能
    """
    logger = get_run_logger()
    logger.info("开始执行端到端测试流程")
    
    # 任务1: 问候
    greeting = hello_task(name)
    
    # 任务2: 数学计算
    calc_result = math_task(10, 32)
    
    # 任务3: 等待任务
    sleep_result = sleep_task(3)
    
    # 汇总结果
    logger.info("所有任务执行完成!")
    logger.info(f"问候结果: {greeting}")
    logger.info(f"计算结果: {calc_result}")
    logger.info(f"等待结果: {sleep_result}")
    
    return {
        "greeting": greeting,
        "calculation": calc_result,
        "sleep": sleep_result,
        "status": "success"
    }

if __name__ == "__main__":
    # 直接运行流程（用于测试）
    result = test_selfhosted_flow()
    print(f"流程执行结果: {result}")
'''
        
        # 写入临时文件
        flow_file = "test_selfhosted_e2e_flow.py"
        with open(flow_file, "w", encoding="utf-8") as f:
            f.write(flow_content)
        
        self.print_status(f"✓ 测试流程文件已创建: {flow_file}", "SUCCESS")
        return flow_file
    
    def test_local_execution(self, flow_file: str):
        """测试流程本地执行"""
        self.print_status("测试流程本地执行...")
        
        try:
            result = self.run_command(f"python {flow_file}")
            self.print_status("✓ 流程本地执行成功", "SUCCESS")
            return True
        except Exception as e:
            self.print_status(f"✗ 流程本地执行失败: {str(e)}", "ERROR")
            return False
    
    def check_work_pool(self):
        """检查工作池状态"""
        self.print_status(f"检查工作池 '{self.work_pool_name}' 状态...")
        
        try:
            result = self.run_command("python -m prefect work-pool ls")
            if self.work_pool_name in result.stdout:
                self.print_status(f"✓ 工作池 '{self.work_pool_name}' 存在", "SUCCESS")
                return True
            else:
                self.print_status(f"✗ 工作池 '{self.work_pool_name}' 不存在", "ERROR")
                return False
        except Exception as e:
            self.print_status(f"✗ 检查工作池失败: {str(e)}", "ERROR")
            return False
    
    def deploy_flow(self, flow_file: str):
        """部署流程到工作池"""
        self.print_status("部署流程到Kubernetes工作池...")
        
        deployment_name = f"{self.test_flow_name}-deployment"
        
        try:
            # 创建部署
            deploy_cmd = f'''python -m prefect deploy {flow_file}:test_selfhosted_flow \\
                --name {deployment_name} \\
                --work-pool {self.work_pool_name} \\
                --image prefecthq/prefect:3-latest \\
                --description "端到端功能测试部署"'''
            
            result = self.run_command(deploy_cmd)
            self.print_status("✓ 流程部署成功", "SUCCESS")
            return deployment_name
        except Exception as e:
            self.print_status(f"✗ 流程部署失败: {str(e)}", "ERROR")
            return None
    
    def run_deployment(self, deployment_name: str):
        """运行部署"""
        self.print_status("触发流程运行...")
        
        try:
            # 运行部署
            run_cmd = f"python -m prefect deployment run {self.test_flow_name}/{deployment_name}"
            result = self.run_command(run_cmd)
            
            # 提取运行ID
            output_lines = result.stdout.split('\n')
            flow_run_id = None
            for line in output_lines:
                if 'Flow run' in line and 'scheduled' in line:
                    # 尝试提取运行ID
                    import re
                    match = re.search(r'Flow run \'([^\']+)\'', line)
                    if match:
                        flow_run_id = match.group(1)
                        break
            
            if flow_run_id:
                self.print_status(f"✓ 流程运行已触发，运行ID: {flow_run_id}", "SUCCESS")
                return flow_run_id
            else:
                self.print_status("✓ 流程运行已触发（未获取到运行ID）", "SUCCESS")
                return "unknown"
                
        except Exception as e:
            self.print_status(f"✗ 流程运行失败: {str(e)}", "ERROR")
            return None
    
    def monitor_flow_run(self, flow_run_id: str = None, timeout: int = 300):
        """监控流程运行状态"""
        self.print_status("监控流程运行状态...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # 获取最近的流程运行
                result = self.run_command("python -m prefect flow-run ls --limit 5")
                
                if "Completed" in result.stdout:
                    self.print_status("✓ 发现已完成的流程运行", "SUCCESS")
                    return True
                elif "Failed" in result.stdout:
                    self.print_status("✗ 发现失败的流程运行", "ERROR")
                    return False
                elif "Running" in result.stdout:
                    self.print_status("● 流程正在运行中...", "INFO")
                else:
                    self.print_status("● 等待流程开始执行...", "INFO")
                
                time.sleep(10)  # 等待10秒再检查
                
            except Exception as e:
                self.print_status(f"监控过程中出错: {str(e)}", "WARNING")
                time.sleep(5)
        
        self.print_status(f"监控超时（{timeout}秒），请手动检查", "WARNING")
        return False
    
    def check_kubernetes_pods(self):
        """检查Kubernetes中的流程执行Pod"""
        self.print_status("检查Kubernetes中的流程执行Pod...")
        
        try:
            # 查看prefect命名空间中的Pod
            result = self.run_command("kubectl get pods -n prefect", check=False)
            
            if "prefect-job" in result.stdout or "flow-run" in result.stdout:
                self.print_status("✓ 发现流程执行Pod", "SUCCESS")
                return True
            else:
                self.print_status("● 未发现流程执行Pod（可能已完成）", "INFO")
                return True
                
        except Exception as e:
            self.print_status(f"检查Pod失败: {str(e)}", "WARNING")
            return False
    
    def get_flow_logs(self):
        """获取流程日志"""
        self.print_status("获取最近的流程日志...")
        
        try:
            # 获取最近的流程运行日志
            result = self.run_command("python -m prefect flow-run logs --head 20", check=False)
            
            if result.stdout.strip():
                self.print_status("流程执行日志:", "INFO")
                print(result.stdout)
                return True
            else:
                self.print_status("没有获取到流程日志", "WARNING")
                return False
                
        except Exception as e:
            self.print_status(f"获取日志失败: {str(e)}", "WARNING")
            return False
    
    def cleanup(self, flow_file: str):
        """清理测试文件"""
        try:
            if os.path.exists(flow_file):
                os.remove(flow_file)
                self.print_status(f"✓ 已清理测试文件: {flow_file}", "INFO")
        except Exception as e:
            self.print_status(f"清理文件失败: {str(e)}", "WARNING")
    
    def run_e2e_test(self):
        """运行完整的端到端测试"""
        self.print_status("=" * 60, "STEP")
        self.print_status("开始 Prefect 自托管端到端功能测试", "STEP")
        self.print_status("=" * 60, "STEP")
        
        test_results = {}
        flow_file = None
        
        try:
            # 1. 配置Prefect客户端
            test_results["client_setup"] = self.setup_prefect_client()
            if not test_results["client_setup"]:
                return test_results
            
            # 2. 创建测试流程
            flow_file = self.create_test_flow()
            test_results["flow_creation"] = True
            
            # 3. 测试本地执行
            test_results["local_execution"] = self.test_local_execution(flow_file)
            
            # 4. 检查工作池
            test_results["work_pool_check"] = self.check_work_pool()
            if not test_results["work_pool_check"]:
                self.print_status("工作池不可用，跳过部署测试", "WARNING")
                return test_results
            
            # 5. 部署流程
            deployment_name = self.deploy_flow(flow_file)
            test_results["deployment"] = deployment_name is not None
            if not test_results["deployment"]:
                return test_results
            
            # 6. 运行流程
            flow_run_id = self.run_deployment(deployment_name)
            test_results["flow_run"] = flow_run_id is not None
            if not test_results["flow_run"]:
                return test_results
            
            # 7. 监控执行
            test_results["execution_success"] = self.monitor_flow_run(flow_run_id)
            
            # 8. 检查Kubernetes Pod
            test_results["k8s_pod_check"] = self.check_kubernetes_pods()
            
            # 9. 获取日志
            test_results["logs"] = self.get_flow_logs()
            
        except Exception as e:
            self.print_status(f"测试过程中发生错误: {str(e)}", "ERROR")
            test_results["error"] = str(e)
        
        finally:
            # 清理
            if flow_file:
                self.cleanup(flow_file)
        
        # 输出测试结果
        self.print_test_summary(test_results)
        return test_results
    
    def print_test_summary(self, results: dict):
        """打印测试结果摘要"""
        self.print_status("=" * 60, "STEP")
        self.print_status("端到端功能测试结果摘要", "STEP")
        self.print_status("=" * 60, "STEP")
        
        test_items = {
            "client_setup": "Prefect客户端配置",
            "flow_creation": "测试流程创建",
            "local_execution": "本地流程执行",
            "work_pool_check": "工作池状态检查",
            "deployment": "流程部署",
            "flow_run": "流程运行触发",
            "execution_success": "流程执行完成",
            "k8s_pod_check": "Kubernetes Pod检查",
            "logs": "流程日志获取"
        }
        
        passed = 0
        total = 0
        
        for key, description in test_items.items():
            if key in results:
                total += 1
                result = results[key]
                status = "PASS" if result else "FAIL"
                color = "SUCCESS" if result else "ERROR"
                self.print_status(f"{description}: {status}", color)
                if result:
                    passed += 1
        
        self.print_status("-" * 60)
        self.print_status(f"总计: {passed}/{total} 通过", 
                         "SUCCESS" if passed == total else "WARNING")
        
        if passed == total:
            self.print_status("🎉 所有功能测试通过！Prefect自托管系统完全正常！", "SUCCESS")
        else:
            self.print_status("⚠️  部分功能测试失败，请检查相关组件", "WARNING")
        
        if "error" in results:
            self.print_status(f"错误信息: {results['error']}", "ERROR")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect自托管端到端功能测试")
    parser.add_argument("--server-url", default="http://localhost:4200",
                       help="Prefect服务器地址")
    parser.add_argument("--api-url", default=None,
                       help="Prefect API地址")
    
    args = parser.parse_args()
    
    # 检查依赖
    try:
        import prefect
        print(f"使用 Prefect 版本: {prefect.__version__}")
    except ImportError:
        print("错误: 缺少Prefect依赖")
        print("请安装: pip install prefect")
        sys.exit(1)
    
    # 运行测试
    tester = PrefectE2ETester(args.server_url, args.api_url)
    results = tester.run_e2e_test()
    
    # 根据结果设置退出码
    all_passed = all(v for k, v in results.items() if k != "error" and isinstance(v, bool))
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()