import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# --- 1. 配置你的 MinIO 连接信息 ---
# 因为我们做了端口转发，所以 endpoint 指向本地的转发端口
MINIO_ENDPOINT = 'http://127.0.0.1:9000'

# !!! 重要：请在这里填入你真实的 MinIO 用户名和密码 !!!
# 如果你不记得了，可以用下面的 kubectl 命令查找
# kubectl get secret minio -n default -o jsonpath="{.data.rootUser}" | base64 --decode
# kubectl get secret minio -n default -o jsonpath="{.data.rootPassword}" | base64 --decode
MINIO_ACCESS_KEY = 'cXFVWCBKY6xlUVjuc8Qk' # 替换成你的 Access Key
MINIO_SECRET_KEY = 'Hx1pYxR6sCHo4NAXqRZ1jlT8Ue6SQk6BqWxz7GKY' # 替换成你的 Secret Key

# --- 2. 创建 S3 客户端 ---
# 我们需要告诉 boto3 不要验证 SSL 证书 (因为是 http)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
)

print("✅ S3 客户端创建成功。")

# --- 3. 测试连接：列出所有的存储桶 (Buckets) ---
try:
    print("\n--- 正在尝试列出所有存储桶 ---")
    response = s3_client.list_buckets()
    
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    if buckets:
        print("✅ 成功连接到 MinIO！可用的存储桶列表：")
        for bucket_name in buckets:
            print(f"  - {bucket_name}")
    else:
        print("✅ 成功连接到 MinIO，但当前没有存储桶。")

except NoCredentialsError:
    print("❌ 错误：凭证信息不正确或未提供。请检查你的 ACCESS_KEY 和 SECRET_KEY。")
except ClientError as e:
    print(f"❌ 客户端错误：无法连接到 MinIO 或进行操作。")
    print(f"   详细信息: {e}")
    print("   请确认：")
    print("   1. `kubectl port-forward svc/minio 9000:9000` 命令是否正在另一个终端运行？")
    print("   2. Endpoint URL 和凭证是否正确？")
except Exception as e:
    print(f"❌ 发生未知错误: {e}")


# --- 4. 测试读取：从 'movielens' 桶下载 'ratings.csv' ---
BUCKET_TO_TEST = 'movielens'
FILE_TO_DOWNLOAD = 'ml-25m/ratings.csv'
LOCAL_FILENAME = 'ratings_from_minio.csv'

if BUCKET_TO_TEST in buckets:
    print(f"\n--- 正在尝试从 '{BUCKET_TO_TEST}' 桶下载 '{FILE_TO_DOWNLOAD}' ---")
    try:
        s3_client.download_file(BUCKET_TO_TEST, FILE_TO_DOWNLOAD, LOCAL_FILENAME)
        print(f"✅ 文件下载成功！已保存为 '{LOCAL_FILENAME}' 在当前目录下。")
        print("   你可以打开这个文件检查内容是否正确。")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"❌ 错误：在 '{BUCKET_TO_TEST}' 桶中没有找到名为 '{FILE_TO_DOWNLOAD}' 的文件。")
        else:
            print(f"❌ 下载文件时出错: {e}")
else:
    print(f"\n--- 跳过文件下载测试，因为名为 '{BUCKET_TO_TEST}' 的存储桶不存在 ---")