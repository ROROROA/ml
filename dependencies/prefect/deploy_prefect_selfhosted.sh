helm repo add prefect https://prefecthq.github.io/prefect-helm
helm repo update

helm upgrade --install prefect-server prefect/prefect-server --values prefect-server-values.yaml 
helm upgrade --install prefect-worker prefect/prefect-worker --values prefect-worker-values-simple.yaml 

#!/bin/bash

# Prefect 自托管部署脚本
# 实现分阶段验证的一键部署方案
# 
# 使用方法: ./deploy_prefect_selfhosted.sh [start|stop|status|logs]

set -e

# 配置变量
NAMESPACE="prefect"
SERVER_RELEASE="prefect-server"
WORKER_RELEASE="prefect-worker" 
HELM_REPO="https://prefecthq.github.io/prefect-helm"
VALUES_DIR="$(dirname "$0")"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要工具
check_prerequisites() {
    print_status "检查必要工具..."
    
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl 未安装"
        exit 1
    fi
    
    if ! command -v helm &> /dev/null; then
        print_error "helm 未安装"
        exit 1
    fi
    
    # 检查kubectl连接
    if ! kubectl cluster-info &> /dev/null; then
        print_error "无法连接到Kubernetes集群"
        exit 1
    fi
    
    print_success "工具检查完成"
}

# 设置Helm仓库
setup_helm_repo() {
    print_status "设置Prefect Helm仓库..."
    
    helm repo add prefect $HELM_REPO &> /dev/null || true
    helm repo update &> /dev/null
    
    print_success "Helm仓库设置完成"
}

# 创建命名空间
create_namespace() {
    print_status "创建命名空间 $NAMESPACE..."
    
    if ! kubectl get namespace $NAMESPACE &> /dev/null; then
        kubectl create namespace $NAMESPACE
        print_success "命名空间 $NAMESPACE 创建成功"
    else
        print_warning "命名空间 $NAMESPACE 已存在"
    fi
}

# 第一阶段：部署Prefect服务器
deploy_server() {
    print_status "=== 第一阶段：部署Prefect服务器 ==="
    
    # 检查values文件
    if [[ ! -f "$VALUES_DIR/prefect-server-values.yaml" ]]; then
        print_error "找不到 prefect-server-values.yaml 文件"
        exit 1
    fi
    
    print_status "安装Prefect服务器..."
    helm upgrade --install $SERVER_RELEASE prefect/prefect-server \
        --namespace $NAMESPACE \
        --values "$VALUES_DIR/prefect-server-values.yaml" \
        --wait --timeout=10m
    
    print_success "Prefect服务器安装完成"
}

# 验证服务器状态
verify_server() {
    print_status "验证Prefect服务器状态..."
    
    # 等待Pod就绪
    print_status "等待服务器Pod就绪..."
    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=prefect-server \
        --namespace=$NAMESPACE --timeout=300s
    
    # 检查服务状态
    print_status "检查服务状态..."
    kubectl get pods,svc -n $NAMESPACE -l app.kubernetes.io/name=prefect-server
    
    # 测试API健康检查
    print_status "测试API连接..."
    kubectl run prefect-api-test --image=curlimages/curl:latest --rm -i --restart=Never \
        --namespace=$NAMESPACE -- \
        curl -f "http://prefect-server.$NAMESPACE.svc.cluster.local:4200/api/health" || {
            print_error "API健康检查失败"
            return 1
        }
    
    print_success "Prefect服务器验证通过"
}

# 第二阶段：部署Worker
deploy_worker() {
    print_status "=== 第二阶段：部署Prefect Worker ==="
    
    # 检查values文件
    if [[ ! -f "$VALUES_DIR/prefect-worker-values.yaml" ]]; then
        print_error "找不到 prefect-worker-values.yaml 文件"
        exit 1
    fi
    
    print_status "安装Prefect Worker..."
    helm upgrade --install $WORKER_RELEASE prefect/prefect-worker \
        --namespace $NAMESPACE \
        --values "$VALUES_DIR/prefect-worker-values.yaml" \
        --wait --timeout=10m
    
    print_success "Prefect Worker安装完成"
}

# 验证Worker状态
verify_worker() {
    print_status "验证Prefect Worker状态..."
    
    # 等待Pod就绪
    print_status "等待Worker Pod就绪..."
    kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=prefect-worker \
        --namespace=$NAMESPACE --timeout=300s
    
    # 检查Worker状态
    print_status "检查Worker状态..."
    kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=prefect-worker
    
    print_success "Prefect Worker验证通过"
}

# 设置端口转发
setup_port_forward() {
    print_status "设置端口转发..."
    
    # 检查是否已有端口转发
    if pgrep -f "kubectl.*port-forward.*prefect-server.*4200" > /dev/null; then
        print_warning "端口转发已存在"
        return 0
    fi
    
    # 在后台启动端口转发
    nohup kubectl port-forward svc/prefect-server 4200:4200 -n $NAMESPACE > /dev/null 2>&1 &
    sleep 3
    
    if pgrep -f "kubectl.*port-forward.*prefect-server.*4200" > /dev/null; then
        print_success "端口转发设置成功，可通过 http://localhost:4200 访问Prefect UI"
    else
        print_error "端口转发设置失败"
        return 1
    fi
}

# 显示状态
show_status() {
    print_status "=== Prefect 自托管状态 ==="
    
    echo
    print_status "Namespace: $NAMESPACE"
    kubectl get namespace $NAMESPACE 2>/dev/null || print_error "命名空间不存在"
    
    echo
    print_status "Prefect服务器状态:"
    kubectl get pods,svc -n $NAMESPACE -l app.kubernetes.io/name=prefect-server 2>/dev/null || print_error "服务器未部署"
    
    echo  
    print_status "Prefect Worker状态:"
    kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=prefect-worker 2>/dev/null || print_error "Worker未部署"
    
    echo
    print_status "端口转发状态:"
    if pgrep -f "kubectl.*port-forward.*prefect-server.*4200" > /dev/null; then
        print_success "端口转发正在运行 (http://localhost:4200)"
    else
        print_warning "端口转发未运行"
    fi
}

# 查看日志
show_logs() {
    local component=${1:-server}
    
    case $component in
        server)
            print_status "Prefect服务器日志:"
            kubectl logs -f -n $NAMESPACE -l app.kubernetes.io/name=prefect-server
            ;;
        worker)
            print_status "Prefect Worker日志:"
            kubectl logs -f -n $NAMESPACE -l app.kubernetes.io/name=prefect-worker
            ;;
        *)
            print_error "未知组件: $component (可选: server, worker)"
            exit 1
            ;;
    esac
}

# 停止服务
stop_services() {
    print_status "=== 停止Prefect服务 ==="
    
    # 停止端口转发
    print_status "停止端口转发..."
    pkill -f "kubectl.*port-forward.*prefect-server.*4200" || true
    
    # 卸载Worker
    if helm list -n $NAMESPACE | grep -q $WORKER_RELEASE; then
        print_status "卸载Prefect Worker..."
        helm uninstall $WORKER_RELEASE -n $NAMESPACE
    fi
    
    # 卸载服务器
    if helm list -n $NAMESPACE | grep -q $SERVER_RELEASE; then
        print_status "卸载Prefect服务器..."
        helm uninstall $SERVER_RELEASE -n $NAMESPACE
    fi
    
    print_success "Prefect服务停止完成"
}

# 完整部署流程
deploy_all() {
    print_status "=== 开始Prefect自托管部署 ==="
    
    check_prerequisites
    setup_helm_repo
    create_namespace
    
    # 第一阶段：部署服务器
    deploy_server
    verify_server
    
    # 第二阶段：部署Worker  
    deploy_worker
    verify_worker
    
    # 设置访问
    setup_port_forward
    
    print_success "=== Prefect自托管部署完成 ==="
    echo
    print_status "访问信息:"
    echo "  - Prefect UI: http://localhost:4200"
    echo "  - API地址: http://localhost:4200/api"
    echo
    print_status "常用命令:"
    echo "  - 查看状态: $0 status"
    echo "  - 查看日志: $0 logs [server|worker]"
    echo "  - 停止服务: $0 stop"
}

# 主函数
main() {
    case "${1:-start}" in
        start)
            deploy_all
            ;;
        stop)
            stop_services
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs $2
            ;;
        server-only)
            check_prerequisites
            setup_helm_repo
            create_namespace
            deploy_server
            verify_server
            setup_port_forward
            ;;
        worker-only)
            check_prerequisites
            setup_helm_repo
            deploy_worker
            verify_worker
            ;;
        *)
            echo "使用方法: $0 [start|stop|status|logs|server-only|worker-only]"
            echo
            echo "命令说明:"
            echo "  start       - 完整部署Prefect自托管服务"
            echo "  stop        - 停止所有Prefect服务"
            echo "  status      - 显示当前状态"
            echo "  logs        - 查看日志 (可选: server, worker)"
            echo "  server-only - 仅部署服务器"
            echo "  worker-only - 仅部署Worker"
            exit 1
            ;;
    esac
}

main "$@"