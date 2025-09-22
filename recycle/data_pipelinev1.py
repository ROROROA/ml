"""
数据处理与特征工程的 Prefect 工作流。
这个文件负责“执行”，它决定是进行每日增量更新还是大规模历史回填，
然后调用 feature_logic.py 中定义的核心逻辑，并触发 Feast 物化。
"""
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation
from datetime import datetime, timedelta
import os

from pyspark.sql import SparkSession, DataFrame

# --- 关键导入：从我们的逻辑模块导入特征计算函数 ---
from etl.feature_logic import calculate_user_rolling_purchase_features

def get_spark_session() -> SparkSession:
    """
    获取或创建一个 SparkSession。
    在生产环境中，这会连接到你的 Spark 集群（如 OpenShift 上的 Spark on K8s）。
    """
    # 在这里可以加入更多配置，如 master URL, executor memory 等
    return SparkSession.builder.appName("FeatureETL").getOrCreate()

@task
def run_feature_etl_job(
    mode: str = "incremental",
    start_date: str = None,
    end_date: str = None
) -> str:
    """
    一个可参数化的ETL任务，可以处理两种模式：
    - incremental: 默认模式，为每日更新计算特征。
    - backfill: 为指定的历史时间范围批量计算特征。
    
    Returns:
        str: 当为增量模式时，返回计算的目标日期。
    """
    spark = get_spark_session()
    logger = get_run_logger()
    logger.info(f"Starting feature ETL job in '{mode}' mode with Spark version {spark.version}.")

    # --- 1. 根据模式，准备“时间骨架”和源数据 ---
    # 假设我们有一张原始购买日志表: raw_data.purchases
    all_purchases_df = spark.table("raw_data.purchases")
    target_date_str = None

    if mode == "incremental":
        target_date = datetime.now() - timedelta(days=1)
        target_date_str = target_date.strftime('%Y-%m-%d')
        logger.info(f"Incremental mode: preparing spine for date {target_date_str}")
        
        # “时间骨架”就是昨天的所有活跃用户
        spine_df = spark.sql(f"""
            SELECT DISTINCT user_id, to_date('{target_date_str}', 'yyyy-MM-dd') as timestamp 
            FROM raw_data.daily_active_users
            WHERE event_date = to_date('{target_date_str}', 'yyyy-MM-dd')
        """)
        
        # 优化：只需读取最近一个多月的数据来满足30天的窗口
        start_filter_date = (target_date - timedelta(days=40)).strftime('%Y-%m-%d')
        source_df = all_purchases_df.filter(f"purchase_date >= to_date('{start_filter_date}', 'yyyy-MM-dd')")

    elif mode == "backfill":
        if not start_date or not end_date:
            raise ValueError("Backfill mode requires start_date and end_date.")
        logger.info(f"Backfill mode: preparing spine from {start_date} to {end_date}")
        
        spine_df = spark.sql(f"""
           SELECT
               user_id,
               explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as timestamp
           FROM raw_data.all_users
        """)
        source_df = all_purchases_df

    else:
        raise ValueError(f"Unknown mode: {mode}")

    # --- 2. 准备调用核心逻辑所需的数据 ---
    joined_df_for_logic = (
        spine_df.withColumn("timestamp_long", spine_df.timestamp.cast("long"))
        .join(
            source_df,
            (spine_df.user_id == source_df.user_id) & (spine_df.timestamp >= source_df.purchase_date),
            "left"
        )
    )

    # --- 3. 调用并复用核心计算逻辑 ---
    final_features_df = calculate_user_rolling_purchase_features(joined_df_for_logic)

    # --- 4. 物化结果 ---
    if mode == "incremental":
        output_table = os.getenv("FEAST_INCREMENTAL_TABLE", "feature_store.user_features_incremental")
        logger.info(f"Saving incremental features to {output_table} for Feast to materialize.")
        final_features_df.write.mode("overwrite").saveAsTable(output_table)
        
    elif mode == "backfill":
        output_table = os.getenv("FEAST_OFFLINE_SOURCE_TABLE", "feature_store.user_features_historical")
        logger.info(f"Saving backfilled features to {output_table} to be used as Feast offline store.")
        final_features_df.write.mode("overwrite").partitionBy("timestamp").saveAsTable(output_table)

    spark.stop()
    return target_date_str

# --- 新增：专门用于调用 Feast 的任务 ---
@task
def materialize_features_to_online_store(target_date: str, feast_repo_path: str = "feature_repo"):
    """
    运行 'feast materialize-incremental' 命令，将最新的特征加载到在线存储中。
    这是连接离线计算和在线服务的关键一步。
    """
    logger = get_run_logger()
    if not target_date:
        logger.warning("Target date is not provided, skipping materialization.")
        return

    # Feast 的 materialize-incremental 命令需要一个结束日期
    command = f"feast -c {feast_repo_path} materialize-incremental {target_date}"
    
    logger.info(f"Running Feast materialization command:\n{command}")
    
    # 使用 Prefect-Shell 来执行命令行指令
    result = ShellOperation(commands=[command]).run()
    
    logger.info("Feast materialization complete.")
    return result


@flow
def data_pipeline_flow():
    """
    这是每日定时执行的 Prefect Flow。
    它现在包含两个步骤：1. Spark ETL, 2. Feast 物化。
    """
    # 步骤 1: 运行 Spark 作业来生产最新的特征数据
    target_date = run_feature_etl_job(mode="incremental")
    
    # 步骤 2: Spark 作业成功后，调用 Feast 将这些新特征推送到在线库
    # Prefect 会自动处理依赖关系，确保此任务在 `run_feature_etl_job` 成功后运行
    materialize_features_to_online_store(target_date=target_date)

@flow
def backfill_flow(start_date: str, end_date: str):
    """
    这是一个手动或按需触发的 Prefect Flow，用于历史数据回填。
    注意：回填流程通常不需要物化到在线库，因为它只为了生成离线训练数据。
    """
    run_feature_etl_job(mode="backfill", start_date=start_date, end_date=end_date)

