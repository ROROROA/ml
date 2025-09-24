# test_ray_connection.py
import ray
import traceback

# --- 在这里配置你要测试的地址 ---
# 测试 1: 使用 Ray Client 端口 (10001)，这是你当前失败的场景
address_to_test = "ray://ray-kuberay-cluster-head-svc.default.svc.cluster.local:10001"

# 测试 2: 使用 GCS 端口 (6379)，作为备选方案
# address_to_test = "ray-kuberay-cluster-head-svc.default.svc.cluster.local:6379"

print(f"--- Attempting to connect to Ray at: {address_to_test} ---")

try:
    # 核心测试代码：调用 ray.init()
    # ignore_version_mismatch=True 是一个有用的调试参数
    ray.init(address=address_to_test)

    # 如果连接成功，打印集群资源信息
    print("\n✅✅✅ Ray connection SUCCESSFUL! ✅✅✅")
    print("\nCluster resources:")
    print(ray.cluster_resources())

except Exception as e:
    # 如果连接失败，打印详细的错误信息
    print(f"\n❌❌❌ Ray connection FAILED! ❌❌❌")
    print(f"\nError Type: {type(e).__name__}")
    print(f"Error Message: {e}")
    print("\nFull Traceback:")
    traceback.print_exc()

finally:
    # 确保无论成功失败都关闭连接
    if ray.is_initialized():
        print("\n--- Shutting down Ray connection ---")
        ray.shutdown()