#!/usr/bin/env python3
"""
Prefect 本地测试流程
测试本地Prefect功能而不依赖于云端服务
"""

from prefect import flow, task
from datetime import datetime
import time

@task
def fetch_data():
    """模拟数据获取任务"""
    print("🔍 开始获取数据...")
    time.sleep(1)  # 模拟处理时间
    data = {
        "users": ["alice", "bob", "charlie"],
        "timestamp": datetime.now().isoformat(),
        "count": 3
    }
    print(f"✅ 数据获取完成: {data['count']} 条记录")
    return data

@task
def process_data(raw_data):
    """模拟数据处理任务"""
    print("⚙️ 开始处理数据...")
    time.sleep(0.5)  # 模拟处理时间
    
    processed = {
        "processed_users": [user.upper() for user in raw_data["users"]],
        "processing_time": datetime.now().isoformat(),
        "original_count": raw_data["count"],
        "processed_count": len(raw_data["users"])
    }
    print(f"✅ 数据处理完成: {processed['processed_count']} 条记录")
    return processed

@task
def save_results(processed_data):
    """模拟保存结果任务"""
    print("💾 开始保存结果...")
    time.sleep(0.3)  # 模拟保存时间
    
    result = {
        "saved_at": datetime.now().isoformat(),
        "records_saved": processed_data["processed_count"],
        "status": "success"
    }
    print(f"✅ 结果保存完成: {result['records_saved']} 条记录")
    return result

@flow(log_prints=True)
def ml_data_pipeline():
    """机器学习数据流水线"""
    print("🚀 启动 ML 数据流水线")
    print(f"📅 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行任务链
    raw_data = fetch_data()
    processed_data = process_data(raw_data)
    final_result = save_results(processed_data)
    
    # 汇总结果
    pipeline_result = {
        "pipeline_status": "completed",
        "execution_time": datetime.now().isoformat(),
        "tasks_completed": 3,
        "final_result": final_result
    }
    
    print("🎉 流水线执行完成!")
    print(f"📊 最终状态: {pipeline_result['pipeline_status']}")
    
    return pipeline_result

if __name__ == "__main__":
    print("🔧 Prefect 本地流程测试")
    print("=" * 50)
    
    try:
        result = ml_data_pipeline()
        print("\n" + "=" * 50)
        print("✅ 测试成功完成!")
        print("📋 执行结果:")
        print(f"   状态: {result['pipeline_status']}")
        print(f"   任务数: {result['tasks_completed']}")
        print(f"   完成时间: {result['execution_time']}")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        print("=" * 50)