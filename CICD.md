架构概览：使用 Argo 实现 Kubernetes 原生的 MLOps CI/CD
本文档详细说明了如何使用 Argo Workflows 和 Argo CD 来替代 GitHub Actions，为我们的 MLOps 框架构建一个端到端的、在 Kubernetes 中运行的自动化流程。

1. 核心组件
Argo Workflows: 一个容器原生的工作流引擎，用于在 Kubernetes 上运行多步骤工作流。我们将用它来定义和执行我们的 CI（持续集成）管道，即“代码变更与自动注册”流程。

Argo CD: 一个声明式的、基于 GitOps 的持续部署工具。它负责将 Git 仓库中定义的期望状态（例如，哪个版本的模型服务应该在运行）同步到 Kubernetes 集群中。我们将用它来实现 CD（持续部署）。

Argo Events (可选，高级): 一个事件驱动的工作流自动化框架。我们可以用它来设置一个“监听器”，当你的 Git 仓库有新的 commit 时，自动触发 Argo Workflows 运行我们的 CI 管道。

2. CI 流程：代码变更与自动注册 (由 Argo Workflows 执行)
这个流程取代了之前的 deploy_prefect_flows.yml 文件。当 main 分支有新的代码合并时，Argo Events 会监听到这个事件，并自动触发一个 Argo Workflow 的运行。

这个 Workflow 会执行以下步骤（定义在 argo-ci-workflow.yml 文件中）：

克隆代码 (Git Clone): 从你的 Git 仓库中拉取最新的 main 分支代码。

构建镜像 (Build Image): 使用 Kaniko 在 Kubernetes Pod 中安全地构建一个新的 Docker 镜像。Kaniko 是一个无需 Docker 守护进程的工具，非常适合在 K8s 环境中使用。镜像会被打上唯一的 Git Commit SHA 标签。

推送镜像 (Push Image): 将构建好的新镜像推送到你的镜像仓库（例如 Harbor, Docker Hub, 或者 OpenShift 内部仓库）。

部署 Flow (Deploy Prefect Flows): 启动一个 Pod，使用刚刚推送的新镜像，运行 prefect deploy 命令，将所有最新的工作流定义（data_pipeline, training_pipeline 等）注册或更新到你的 Prefect 服务器。

3. CD 流程：模型服务的自动部署 (由 Argo CD 管理)
这是我们框架的下一步。在你训练并提拔了一个生产模型后，CI 流程可以自动更新一个配置文件，指明新的模型服务应该使用哪个镜像版本。Argo CD 会持续监控这个配置文件。

GitOps 原则: 你的模型服务（一个 FastAPI 应用）的所有 Kubernetes 部署配置（Deployment, Service, Ingress 等 YAML 文件）都存储在 Git 仓库中。Git 是唯一的事实来源。

自动同步: Argo CD 会检测到 Git 仓库中的变更（比如镜像标签从 v1.0 更新到了 v1.1）。

安全部署: Argo CD 会自动地、安全地将这个变更应用到你的 Kubernetes 集群，可能会采用蓝绿部署或金丝雀发布等策略，实现模型的平滑上线。

argocd-model-service-app.yml 文件就是用来告诉 Argo CD：“请监控我的 Git 仓库，并确保我的模型服务始终与仓库中定义的配置保持一致。”

这个 Argo 驱动的架构为你提供了一个完全在 Kubernetes 内部运行的、强大、可扩展且自动化的 MLOps 平台。