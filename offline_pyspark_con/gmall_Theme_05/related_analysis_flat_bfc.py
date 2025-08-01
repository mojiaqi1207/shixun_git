# import os
# # 设置Python环境（根据实际路径修改）
# os.environ["PYSPARK_PYTHON"] = "D:/Anaconda/envs/offline_pyspark_con/python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Anaconda/envs/offline_pyspark_con/python.exe"
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, countDistinct, count, sum, collect_list, struct,
#     row_number, date_sub, current_date, when, array_contains, explode, to_json
# )
# from pyspark.sql.window import Window
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("商品主题连带分析-不分层实现") \
#     .master("local[*]") \
#     .getOrCreate()
#
# # 工单编号：大数据-电商数仓-05-商品主题连带分析看板
#
# # 1. 读取并筛选近7天数据
# raw_df = spark.read.csv("mock_related_products_data", header=True, inferSchema=True)
# analysis_start = date_sub(current_date(), 7)  # 近7天分析起点
# df = raw_df.filter(
#     (col("user_id").isNotNull()) &
#     (col("product_id").isNotNull()) &
#     ((col("visit_time") >= analysis_start) |
#      (col("collect_time") >= analysis_start) |
#      (col("pay_time") >= analysis_start))
# )
#
# # 2. 识别TOP10主商品（引流能力最强、销量最高）
# product_metrics = df.groupBy("product_id", "product_name") \
#     .agg(
#     countDistinct("user_id").alias("uv"),  # 引流能力指标（访客数）
#     count("pay_time").alias("pay_count")   # 销量指标（支付次数）
# )
# window_top_main = Window.orderBy(col("uv").desc(), col("pay_count").desc())
# top_main = product_metrics.withColumn("rank", row_number().over(window_top_main)) \
#     .filter(col("rank") <= 10) \
#     .select("product_id").withColumnRenamed("product_id", "main_product_id")
#
# # 3. 分析主商品与关联商品的关联关系
# # 3.1 按用户分组，收集包含行为时间的商品明细
# user_product_list = df.groupBy("user_id") \
#     .agg(
#     collect_list(
#         struct(
#             "product_id",
#             "visit_time",
#             "collect_time",
#             "pay_time"
#         )
#     ).alias("product_details")  # 商品ID+行为时间的结构体列表
# )
#
# # 3.2 提取TOP主商品及对应的用户行为
# main_products = user_product_list.select(
#     col("user_id"),
#     col("product_details"),  # 保留商品明细列表，用于后续提取关联商品
#     explode(col("product_details")).alias("main_product")  # 拆解主商品
# ).join(
#     top_main,
#     col("main_product.product_id") == col("main_product_id"),  # 筛选TOP主商品
#     "inner"
# )
#
# # 3.3 提取关联商品（排除主商品自身）
# related_pairs = main_products.select(
#     col("user_id"),
#     col("main_product_id"),
#     col("main_product").alias("main_product_details"),  # 主商品行为明细
#     explode(col("product_details")).alias("related_product_details")  # 拆解关联商品
# ).filter(
#     col("related_product_details.product_id") != col("main_product_id")  # 排除自身关联
# )
#
# # 3.4 计算关联指标（同时访问/收藏/支付）并取TOP30关联商品
# window_related = Window.partitionBy("main_product_id").orderBy(col("co_pay_count").desc())
# flat_relation = related_pairs.groupBy(
#     col("main_product_id"),
#     col("related_product_details.product_id").alias("related_product_id")
# ).agg(
#     # 同时访问次数
#     count(when(
#         col("main_product_details.visit_time").isNotNull() &
#         col("related_product_details.visit_time").isNotNull(), 1
#     )).alias("co_visit_count"),
#     # 同时收藏次数
#     count(when(
#         col("main_product_details.collect_time").isNotNull() &
#         col("related_product_details.collect_time").isNotNull(), 1
#     )).alias("co_collect_count"),
#     # 同时支付次数
#     count(when(
#         col("main_product_details.pay_time").isNotNull() &
#         col("related_product_details.pay_time").isNotNull(), 1
#     )).alias("co_pay_count")
# ).withColumn("related_rank", row_number().over(window_related)) \
#     .filter(col("related_rank") <= 30)  # 取TOP30关联商品
#
# # 4. 计算连带效果（引导能力TOP10商品）
# flat_guide = flat_relation.groupBy("main_product_id") \
#     .agg(
#     sum("co_visit_count").alias("total_guide_visits"),  # 总引导访问量
#     collect_list(
#         struct(
#             "related_product_id",
#             "co_visit_count",
#             "co_collect_count",
#             "co_pay_count"
#         )
#     ).alias("related_details")  # 关联商品明细
# ) \
#     .join(
#     df.select("product_id", "product_name").distinct(),
#     col("main_product_id") == col("product_id"),
#     "inner"
# ) \
#     .orderBy(col("total_guide_visits").desc()) \
#     .limit(10) \
#     .select(
#     "main_product_id",
#     "product_name",
#     "total_guide_visits",
#     "related_details"
# )
#
# # 5. 数据保存（使用Parquet格式存储复杂类型，CSV存储简单类型）
# try:
#     # 保存关联洞察结果（简单类型，可用CSV）
#     flat_relation.write \
#         .mode("overwrite") \
#         .option("header", "true") \
#         .csv("flat_relation_result")
#     print("关联洞察结果已保存至 flat_relation_result")
#
#     # 保存连带效果结果（含复杂类型，用Parquet）
#     flat_guide.write \
#         .mode("overwrite") \
#         .parquet("flat_guide_result")
#     print("连带效果结果已保存至 flat_guide_result")
#
#     # 可选：将复杂类型转换为JSON字符串后保存为CSV（方便非Spark工具查看）
#     flat_guide.withColumn("related_details_json", to_json(col("related_details"))) \
#         .select("main_product_id", "product_name", "total_guide_visits", "related_details_json") \
#         .write \
#         .mode("overwrite") \
#         .option("header", "true") \
#         .csv("flat_guide_csv_result")
#     print("连带效果JSON格式结果已保存至 flat_guide_csv_result")
#
# except Exception as e:
#     print(f"保存数据时出错：{str(e)}")
#
# # 6. 展示部分结果
# print("\n===== 关联洞察TOP30结果示例 =====")
# flat_relation.show(5, truncate=False)
#
# print("\n===== 连带效果TOP10结果示例 =====")
# flat_guide.show(5, truncate=False)
#
# spark.stop()