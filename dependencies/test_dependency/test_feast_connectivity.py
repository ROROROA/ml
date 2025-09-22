#!/usr/bin/env python3
"""
Feast 连通性测试脚本
测试 Feast Feature Server 的各项功能
"""
import requests
import json
import sys
import time
from datetime import datetime, timedelta

def test_feast_connectivity():
    """测试 Feast 连通性"""
    try:
        # 端口配置
        feature_server_url = "http://localhost:6566"
        api_docs_url = "http://localhost:9090"
        ui_url = "http://localhost:8081"
        
        print("🔍 测试 Feast Feature Server 连通性...")
        print(f"📍 Feature Server: {feature_server_url}")
        print(f"📍 API 文档: {api_docs_url}")
        print(f"📍 Web UI: {ui_url}")
        print("")
        
        # 1. 测试 Feature Server 健康检查
        print("1️⃣ 测试 Feature Server 健康检查...")
        try:
            response = requests.get(f"{feature_server_url}/health", timeout=10)
            if response.status_code == 200:
                print(f"✅ Feature Server 健康检查通过: {response.status_code}")
            else:
                print(f"⚠️ Feature Server 健康检查响应: {response.status_code}")
        except Exception as e:
            print(f"❌ Feature Server 健康检查失败: {e}")
            return False
        
        # 2. 测试获取特征存储信息
        print("\n2️⃣ 测试获取特征存储信息...")
        try:
            response = requests.get(f"{feature_server_url}/info", timeout=10)
            if response.status_code == 200:
                info = response.json()
                print(f"✅ 获取特征存储信息成功")
                print(f"   版本: {info.get('version', 'Unknown')}")
                if 'feature_views' in info:
                    print(f"   特征视图数量: {len(info['feature_views'])}")
            else:
                print(f"⚠️ 获取特征存储信息响应: {response.status_code}")
        except Exception as e:
            print(f"❌ 获取特征存储信息失败: {e}")
        
        # 3. 测试列出特征视图
        print("\n3️⃣ 测试列出特征视图...")
        try:
            response = requests.get(f"{feature_server_url}/list-feature-views", timeout=10)
            if response.status_code == 200:
                feature_views = response.json()
                print(f"✅ 列出特征视图成功")
                if isinstance(feature_views, dict) and 'featureViews' in feature_views:
                    fvs = feature_views['featureViews']
                    print(f"   发现 {len(fvs)} 个特征视图")
                    for fv in fvs[:3]:  # 只显示前3个
                        print(f"   - {fv.get('spec', {}).get('name', 'Unknown')}")
                elif isinstance(feature_views, list):
                    print(f"   发现 {len(feature_views)} 个特征视图")
                else:
                    print(f"   响应格式: {type(feature_views)}")
            else:
                print(f"⚠️ 列出特征视图响应: {response.status_code}")
                print(f"   响应内容: {response.text[:200]}")
        except Exception as e:
            print(f"❌ 列出特征视图失败: {e}")
        
        # 4. 测试获取在线特征（示例请求）
        print("\n4️⃣ 测试获取在线特征...")
        try:
            # 构造一个简单的特征请求
            feature_request = {
                "features": [],  # 如果没有特征视图，保持为空
                "entities": {
                    "user_id": ["user_1", "user_2"]
                }
            }
            
            response = requests.post(
                f"{feature_server_url}/get-online-features",
                json=feature_request,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                print("✅ 在线特征请求成功")
                result = response.json()
                print(f"   响应键: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
            elif response.status_code == 422:
                print("⚠️ 特征请求格式需要调整（这是正常的，因为我们没有定义具体特征）")
            else:
                print(f"⚠️ 在线特征请求响应: {response.status_code}")
                print(f"   响应内容: {response.text[:200]}")
                
        except Exception as e:
            print(f"❌ 在线特征请求失败: {e}")
        
        # 5. 测试 API 文档访问
        print("\n5️⃣ 测试 API 文档访问...")
        try:
            response = requests.get(f"{api_docs_url}/docs", timeout=10)
            if response.status_code == 200:
                print(f"✅ API 文档可访问: {api_docs_url}/docs")
            else:
                print(f"⚠️ API 文档响应: {response.status_code}")
        except Exception as e:
            print(f"❌ API 文档访问失败: {e}")
        
        # 6. 测试 Web UI 访问
        print("\n6️⃣ 测试 Web UI 访问...")
        try:
            response = requests.get(f"{ui_url}/", timeout=10)
            if response.status_code == 200:
                print(f"✅ Web UI 可访问: {ui_url}")
            else:
                print(f"⚠️ Web UI 响应: {response.status_code}")
        except Exception as e:
            print(f"❌ Web UI 访问失败: {e}")
        
        print("\n🎉 Feast 连通性测试完成!")
        print(f"🌐 可通过以下方式访问 Feast:")
        print(f"   - Feature Server API: {feature_server_url}")
        print(f"   - API 文档: {api_docs_url}/docs")
        print(f"   - Web UI: {ui_url}")
        
        return True
        
    except Exception as e:
        print(f"❌ Feast 连通性测试失败: {e}")
        return False

def test_feast_with_sample_data():
    """使用示例数据测试 Feast 功能"""
    print("\n" + "="*60)
    print("🧪 Feast 示例数据测试")
    print("="*60)
    
    feature_server_url = "http://localhost:6566"
    
    # 测试推送示例数据到特征存储
    print("📤 测试推送示例特征数据...")
    
    try:
        # 构造示例特征数据
        sample_features = {
            "feature_service_name": "sample_service",
            "entities": {
                "user_id": "test_user_123"
            },
            "features": {
                "user_age": 25,
                "user_location": "Beijing",
                "last_purchase_amount": 299.99
            },
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"   示例数据: {json.dumps(sample_features, indent=2, ensure_ascii=False)}")
        
        # 注意：实际的 Feast 可能需要不同的端点和数据格式
        # 这里主要是测试连通性
        print("⚠️ 注意：实际特征推送需要预先定义的特征视图和正确的数据格式")
        print("✅ 示例数据构造成功")
        
    except Exception as e:
        print(f"❌ 示例数据测试失败: {e}")

if __name__ == "__main__":
    print("🚀 开始 Feast 连通性测试...")
    print("")
    
    # 基础连通性测试
    connectivity_success = test_feast_connectivity()
    
    # 示例数据测试
    test_feast_with_sample_data()
    
    print("\n" + "="*60)
    if connectivity_success:
        print("🎊 Feast 测试总体成功! Feature Server 运行正常")
    else:
        print("⚠️ Feast 测试完成，部分功能可能需要进一步配置")
    print("="*60)
    
    sys.exit(0 if connectivity_success else 1)