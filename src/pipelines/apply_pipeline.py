"""
本模块定义了一个用于应用 Feast 特征仓库变更的 Prefect 工作流。
这是一个配置流（Configuration Flow），负责将代码中的特征定义同步到 Feast 注册表。
"""
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation


@task
def apply_feast_definitions(feast_repo_path: str = "feature_repo"):
    """
    在一个任务中执行 `feast apply` 命令。
    """
    logger = get_run_logger()
    command = f"feast -c {feast_repo_path} apply"
    
    logger.info(f"Running Feast apply command to sync definitions:\n{command}")
    
    # 使用 Prefect-Shell 来执行命令行指令，并流式传输日志
    # 在生产环境中，需要确保运行此任务的 Agent/Worker 有权限访问 Feast 注册表
    result = ShellOperation(
        commands=[command],
        stream_output=True
    ).run()
    
    logger.info("Feast apply completed successfully.")
    return result

@flow
def apply_pipeline_flow():
    """
    一个专门用于将最新的特征定义应用到生产环境的工作流。
    """
    get_run_logger().info("Starting Feast Apply Pipeline...")
    apply_feast_definitions()


### 如何将这个新流程融入你的 MLOps 生命周期

# 现在你有了一个新的 `apply_pipeline_flow`，你可以这样使用它：

# 1.  **作为独立流程运行：**
#     * 当你的算法工程师修改了 `feature_repo/` 中的任何特征定义后，他们可以**手动触发**一次 `apply_pipeline_flow`。
#     * 一旦这个流程成功运行，就代表生产环境的 Feast 注册表已经更新，做好了接受新特征数据的准备。

# 2.  **作为其他流程的前置依赖：**
#     * 正如你所设想的，你可以创建一个更高阶的“元流程”（meta-flow）来确保执行顺序。

#     # ```python
#     # 这是一个如何组织流程的示例，可以放在一个 master_flow.py 文件里
#     from prefect import flow
#     from src.pipelines.apply_pipeline import apply_pipeline_flow
#     from src.pipelines.data_pipeline import data_pipeline_flow

#     @flow
#     def daily_master_flow():
#         """
#         一个更高阶的流程，确保在运行数据管道前，特征定义总是最新的。
#         注意：这会导致 feast apply 每天都被运行，可能会有些冗余。
#         """
#         # 步骤 1: 应用最新的特征定义
#         apply_task = apply_pipeline_flow.submit()

#         # 步骤 2: 等待 apply 成功后，运行每日增量数据管道
#         data_pipeline_flow.submit(wait_for=[apply_task])
    
