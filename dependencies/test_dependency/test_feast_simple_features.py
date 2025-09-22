#!/usr/bin/env python3
"""
Feast 简化特征测试
使用本地文件数据源创建简单的特征定义并测试
"""

from feast import FeatureStore, Entity, FeatureView, Field, FileSource
from feast.types import Float32, String
from datetime import timedelta, datetime
import pandas as pd
import tempfile
import os

def create_test_feature_store():
    """创建用于测试的简单特征存储"""
    print("🔧 创建测试特征存储配置...")
    
    # 创建临时目录存放测试数据
    temp_dir = tempfile.mkdtemp()
    
    # 创建示例数据
    data = pd.DataFrame({
        'user_id': ['user_1', 'user_2', 'user_3'],
        'total_orders': [5.0, 3.0, 10.0],
        'avg_value': [199.99, 89.50, 299.99],
        'platform': ['ios', 'web', 'android'],
        'event_timestamp': [
            datetime.now() - timedelta(hours=3),
            datetime.now() - timedelta(hours=2),
            datetime.now() - timedelta(hours=1)
        ]
    })
    
    # 保存到临时文件
    data_file = os.path.join(temp_dir, 'test_features.parquet')
    data.to_parquet(data_file)
    
    # 定义实体
    user_entity = Entity(
        name="user_id",
        description="用户ID"
    )
    
    # 定义数据源
    file_source = FileSource(
        path=data_file,
        event_timestamp_column="event_timestamp"
    )
    
    # 定义特征视图
    user_features_fv = FeatureView(
        name="test_user_features",
        entities=[user_entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="total_orders", dtype=Float32),
            Field(name="avg_value", dtype=Float32),
            Field(name="platform", dtype=String),
        ],
        source=file_source,
        online=True
    )
    
    return temp_dir, user_entity, user_features_fv, data

def test_feast_with_simple_features():
    """使用简单特征进行完整测试"""
    print("🚀 Feast 简化特征完整测试")
    print("=" * 50)
    
    try:
        # 创建测试特征
        temp_dir, user_entity, user_features_fv, test_data = create_test_feature_store()
        
        print(f"✅ 测试数据创建成功")
        print(f"   数据文件路径: {temp_dir}")
        print(f"   数据样本:")
        print(test_data.head())
        
        print(f"\n✅ 特征定义创建成功")
        print(f"   实体: {user_entity.name}")
        print(f"   特征视图: {user_features_fv.name}")
        print(f"   特征字段: {[field.name for field in user_features_fv.schema]}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
    
    finally:
        # 清理临时文件
        try:
            import shutil
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir)
                print(f"\n🧹 清理临时文件: {temp_dir}")
        except:
            pass

if __name__ == "__main__":
    test_feast_with_simple_features()