# # 工单编号: 大数据-电商数仓-06-商品主题新品追踪看板
# # 不分层实现方式：直接基于原始数据计算所有指标，不进行ODS/DWD/DWS分层处理
# # 技术栈：Spark 3.2+、PySpark+SparkSQL
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum, count, avg, max, min, datediff, current_date, lit, when, date_format, year, month, dayofmonth, dayofweek
# from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
# from pyspark.sql.window import Window
# import os
# os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# # 初始化SparkSession
# def init_spark():
#     return SparkSession.builder \
#         .appName("电商数仓-商品主题新品追踪看板-不分层实现") \
#         .master("local[*]") \
#         .getOrCreate()
#
# # 生成模拟原始数据（模拟电商平台商品信息及交易记录）
# def generate_raw_data(spark):
#     # 定义原始数据结构（匹配文档中原始数据字段）
#     schema = StructType([
#         StructField("product_id", StringType(), True),  # 商品货号
#         StructField("title", StringType(), True),       # 商品标题
#         StructField("category", StringType(), True),    # 商品类目
#         StructField("main_image", StringType(), True),  # 商品主图路径
#         StructField("putaway_date", DateType(), True),  # 上架日期
#         StructField("price", DoubleType(), True),       # 售价
#         StructField("sales", IntegerType(), True),      # 销量
#         StructField("pay_amount", DoubleType(), True),  # 支付金额
#         StructField("tag", StringType(), True),         # 新品标签（普通新品/小黑盒新品）
#         StructField("is_tmall_new", IntegerType(), True)# 是否为天猫新品（1是/0否）
#     ])
#
#     # 生成模拟数据（1000条示例数据）
#     data = []
#     from datetime import datetime, timedelta
#     import random
#     categories = ["电子产品", "服装鞋帽", "家居用品", "美妆个护", "食品饮料"]
#     tags = ["普通新品", "小黑盒新品"]
#
#     for i in range(1000):
#         # 随机生成上架日期（近90天内）
#         days_ago = random.randint(0, 90)
#         putaway_date = (datetime.now() - timedelta(days=days_ago)).date()
#
#         # 随机生成商品数据
#         data.append({
#             "product_id": f"P{20250000 + i}",
#             "title": f"{random.choice(categories)}新品{i+1}",
#             "category": random.choice(categories),
#             "main_image": f"/images/{20250000 + i}.jpg",
#             "putaway_date": putaway_date,
#             "price": round(random.uniform(99.9, 1999.9), 2),
#             "sales": random.randint(0, 500),
#             "pay_amount": round(random.uniform(0, 100000), 2),
#             "tag": random.choice(tags),
#             "is_tmall_new": random.randint(0, 1)  # 10%概率为天猫新品
#         })
#
#     return spark.createDataFrame(data, schema)
#
# # 1. 最近上新滚动展示模块
# def recent_new_products(raw_df):
#     """展示30天内上架新品的主图、标题、货号及30天累计支付金额"""
#     # 定义窗口：按商品ID分区（确保同一件商品的金额累计正确）
#     product_window = Window.partitionBy("product_id")
#
#     return raw_df.filter(
#         datediff(current_date(), col("putaway_date")) <= 30  # 筛选30天内上架商品
#     ).select(
#         col("product_id"),
#         col("title"),
#         col("main_image"),
#         col("putaway_date"),
#         sum("pay_amount").over(product_window).alias("thirty_days_pay_amount")  # 使用窗口函数
#     ).dropDuplicates(["product_id"])  \
#     .orderBy(col("putaway_date").desc())  # 按上架日期倒序
#
# # 2. 新品监控模块
# def new_product_monitor(raw_df, start_date, end_date):
#     """基于所选周期，汇总天猫新品的整体数据"""
#     return raw_df.filter(
#         (col("is_tmall_new") == 1) &  # 筛选天猫新品
#         (col("putaway_date").between(start_date, end_date)) &  # 筛选周期内上架
#         (datediff(current_date(), col("putaway_date")) <= 30)  # 仅统计上架30天内数据
#     ).agg(
#         count("product_id").alias("new_product_count"),  # 上新商品数
#         sum("sales").alias("total_sales"),  # 累计销售件数
#         sum("pay_amount").alias("total_pay_amount"),  # 累计支付金额
#         avg("price").alias("avg_price"),  # 平均单价
#         max("pay_amount").alias("max_single_sales"),  # 最高单品销售额
#         min("pay_amount").alias("min_single_sales"),  # 最低单品销售额
#         count(when(col("sales") > 0, True)).alias("has_sales_count")  # 有销量商品数
#     ).withColumn("statistics_period", lit(f"{start_date}至{end_date}"))  # 添加统计周期字段
#
# # 3. 新品全年复盘模块
# def yearly_review(raw_df, target_year):
#     """生成全年新品支付金额热力图数据"""
#     # 导入必要的日期处理类
#     from datetime import datetime, timedelta
#     from pyspark.sql import functions as F
#     from pyspark.sql.types import DateType
#
#     # 生成全年日期维度表
#     date_range = raw_df.sparkSession.createDataFrame(  # 使用raw_df的sparkSession避免变量未定义
#         [(datetime(int(target_year), 1, 1) + timedelta(days=i)).date()
#          for i in range(366)],  # 包含闰年
#         DateType()
#     ).withColumnRenamed("value", "putaway_date")
#
#     # 计算每日上架新品的30天累计支付金额
#     daily_data = raw_df.filter(
#         (col("is_tmall_new") == 1) &  # 筛选天猫新品
#         (year(col("putaway_date")) == target_year)  # 筛选目标年份
#     ).groupBy("putaway_date").agg(
#         sum("pay_amount").alias("total_pay_amount")
#     )
#
#     # 后续逻辑...（保持原代码其他部分不变）
#     # 关联日期维度表（确保全年每天都有数据）
#     full_data = date_range.join(
#         daily_data, on="putaway_date", how="left"
#     ).fillna(0, subset=["total_pay_amount"])  # 无新品日期填充0
#
#     # 计算热力等级（1-5级）
#     max_amount = full_data.agg(max("total_pay_amount")).collect()[0][0]
#     return full_data.withColumn(
#         "year", year(col("putaway_date"))
#     ).withColumn(
#         "month", month(col("putaway_date"))
#     ).withColumn(
#         "day", dayofmonth(col("putaway_date"))
#     ).withColumn(
#         "month_day", date_format(col("putaway_date"), "MM-dd")
#     ).withColumn(
#         "weekday", dayofweek(col("putaway_date"))
#     ).withColumn(
#         # 根据支付金额占比计算热力等级
#         "heat_level",
#         when(col("total_pay_amount") == 0, 1)
#         .when(col("total_pay_amount") / max_amount <= 0.2, 1)
#         .when(col("total_pay_amount") / max_amount <= 0.4, 2)
#         .when(col("total_pay_amount") / max_amount <= 0.6, 3)
#         .when(col("total_pay_amount") / max_amount <= 0.8, 4)
#         .otherwise(5)
#     )
#
# # 4. 新品列表模块
# def new_product_list(raw_df, tag_filter=None):
#     # 定义窗口：按商品ID分区（确保同一件商品的聚合正确）
#     product_window = Window.partitionBy("product_id")
#
#     filtered_df = raw_df.filter(
#         datediff(current_date(), col("putaway_date")) <= 30
#     )
#
#     if tag_filter:
#         filtered_df = filtered_df.filter(col("tag") == tag_filter)
#
#     return filtered_df.select(
#         col("product_id"),
#         col("title"),
#         col("category"),
#         col("tag"),
#         col("putaway_date"),
#         col("price"),
#         sum("sales").over(product_window).alias("sales"),  # 使用窗口函数
#         sum("pay_amount").over(product_window).alias("thirty_days_pay_amount")
#     ).dropDuplicates(["product_id"])  # 修正：用列表包裹字段名
# # 主函数
# if __name__ == "__main__":
#     # 初始化Spark
#     spark = init_spark()
#
#     # 生成模拟原始数据
#     raw_data = generate_raw_data(spark)
#     print("原始数据示例：")
#     raw_data.show(5)
#
#     # 1. 最近上新滚动展示
#     recent_products = recent_new_products(raw_data)
#     print("\n最近30天上新商品：")
#     recent_products.select("product_id", "title", "putaway_date", "thirty_days_pay_amount").show(5)
#
#     # 2. 新品监控（示例周期：2025-01-01至2025-01-31）
#     monitor_data = new_product_monitor(raw_data, "2025-04-01", "2025-04-15")
#     print("\n新品监控数据：")
#     monitor_data.show()
#
#     # 3. 新品全年复盘（示例年份：2025）
#     yearly_data = yearly_review(raw_data, 2025)
#     print("\n全年复盘热力图数据（前5行）：")
#     yearly_data.select("putaway_date", "total_pay_amount", "heat_level").show(5)
#
#     # 4. 新品列表（筛选小黑盒新品）
#     black_box_products = new_product_list(raw_data, "小黑盒新品")
#     print("\n小黑盒新品列表（前5行）：")
#     black_box_products.select("product_id", "title", "category", "thirty_days_pay_amount").show(5)
#
#     # 停止Spark
#     spark.stop()