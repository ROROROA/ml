# import os
# import boto3
# from botocore.exceptions import ClientError

# # --- 配置信息 (请根据你的情况修改) ---
# MINIO_ENDPOINT_URL = "http://minio.default.svc.cluster.local:9000"
# BUCKET_NAME = "feast"  # 你在 MinIO 中用于 Feast 的存储桶名称
# REGION_NAME = "us-east-1" # 对于 MinIO，这个值可以任意填写，但不能为空

# # 从环境变量中获取凭证 (脚本会自动读取)
# aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
# aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# if not aws_access_key_id or not aws_secret_access_key:
#     print("❌ 错误：请先设置 AWS_ACCESS_KEY_ID 和 AWS_SECRET_ACCESS_KEY 环境变量！")
#     exit()

# print(f"--- 正在使用 Access Key '{aws_access_key_id[:5]}...' 连接到 MinIO ---")
# print(f"--- Endpoint: {MINIO_ENDPOINT_URL} ---")
# print(f"--- Bucket: {BUCKET_NAME} ---\n")

# try:
#     # 创建一个 S3 客户端，指向你的 MinIO
#     s3_client = boto3.client(
#         's3',
#         endpoint_url=MINIO_ENDPOINT_URL,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=REGION_NAME
#     )

#     # 1. 测试 HeadBucket (这正是 Feast 失败的地方)
#     print("--- 1. 正在测试 HeadBucket (检查存储桶权限)... ---")
#     try:
#         s3_client.head_bucket(Bucket=BUCKET_NAME)
#         print(f"✅ 成功! 你的用户有权限访问存储桶 '{BUCKET_NAME}'。\n")
#     except ClientError as e:
#         print(f"❌ 失败! HeadBucket 操作被拒绝。")
#         print(f"   错误代码: {e.response['Error']['Code']}")
#         print(f"   错误信息: {e.response['Error']['Message']}\n")
#         # 如果这里失败，后面的测试也基本会失败
        
#     # 2. 测试 ListObjects (列出桶内对象)
#     print(f"--- 2. 正在测试 ListObjects (列出 '{BUCKET_NAME}' 内的文件)... ---")
#     try:
#         response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=5)
#         print(f"✅ 成功! 可以列出存储桶中的对象。")
#         objects = [obj['Key'] for obj in response.get('Contents', [])]
#         if objects:
#             print(f"   桶内文件示例: {objects}\n")
#         else:
#             print("   存储桶当前为空。\n")
#     except ClientError as e:
#         print(f"❌ 失败! ListObjects 操作被拒绝。")
#         print(f"   错误代码: {e.response['Error']['Code']}")
#         print(f"   错误信息: {e.response['Error']['Message']}\n")
        
#     # 3. 测试 PutObject (上传一个测试文件)
#     test_file_key = "test_from_script.txt"
#     print(f"--- 3. 正在测试 PutObject (上传文件 '{test_file_key}')... ---")
#     try:
#         s3_client.put_object(Bucket=BUCKET_NAME, Key=test_file_key, Body=b"Hello MinIO!")
#         print(f"✅ 成功! 文件 '{test_file_key}' 已上传。\n")
        
#         # 4. 测试 GetObject (下载刚上传的文件)
#         print(f"--- 4. 正在测试 GetObject (下载文件 '{test_file_key}')... ---")
#         obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=test_file_key)
#         content = obj['Body'].read().decode('utf-8')
#         print(f"✅ 成功! 读取到文件内容: '{content}'\n")

#         # 5. 测试 DeleteObject (删除测试文件)
#         print(f"--- 5. 正在测试 DeleteObject (删除文件 '{test_file_key}')... ---")
#         # s3_client.delete_object(Bucket=BUCKET_NAME, Key=test_file_key)
#         print(f"✅ 成功! 文件 '{test_file_key}' 已删除。\n")

#     except ClientError as e:
#         # 捕获上传、下载、删除过程中的权限错误
#         print(f"❌ 在读写操作中失败!")
#         print(f"   错误代码: {e.response['Error']['Code']}")
#         print(f"   错误信息: {e.response['Error']['Message']}\n")


# except Exception as e:
#     print(f"💥 发生了一个意料之外的错误，可能是网络连接问题或 Endpoint URL 不正确。")
#     print(f"   详细错误: {e}")
# import socket # 引入 socket 库来获取本机IP
# def get_host_ip():
#     """获取本机在局域网中的IP地址"""
#     s = None
#     try:
#         s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#         # 连接到一个公共DNS，但这并不会真的发送数据
#         s.connect(('8.8.8.8', 80))
#         ip = s.getsockname()[0]
#     finally:
#         if s:
#             s.close()
#     return ip
# print(get_host_ip())