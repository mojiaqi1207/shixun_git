# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, countDistinct, count, when, collect_list, struct,
#     row_number, date_sub, current_date, explode
# )
# from pyspark.sql.window import Window
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("引流款热销款关联宝贝分析") \
#     .master("local[*]") \
#     .getOrCreate()
#
# # 根据近7天的“引流款”和“热销款”主商品，洞察出与主商品有同时访问、同时收藏加购和同时段支付的top30宝贝
# # 读取数据（替换为实际数据路径）
# # 数据字段需包含：user_id, product_id, product_name, visit_time, collect_time, pay_time
# df = spark.read.csv("mock_related_products_data", header=True, inferSchema=True)
#
# # 1. 筛选近7天数据
# analysis_start_date = date_sub(current_date(), 7)
# recent_df = df.filter(
#     (col("visit_time") >= analysis_start_date) |
#     (col("collect_time") >= analysis_start_date) |
#     (col("pay_time") >= analysis_start_date)
# )
#
# # 2. 识别引流款和热销款主商品（TOP10）
# # 2.1 计算商品核心指标（引流能力：访客数；热销程度：支付次数）
# product_metrics = recent_df.groupBy("product_id", "product_name") \
#     .agg(
#     countDistinct("user_id").alias("uv"),  # 引流能力指标
#     count("pay_time").alias("pay_count")   # 热销程度指标
# )
#
# # 2.2 筛选TOP10主商品（综合引流和热销）
# window_top_main = Window.orderBy(col("uv").desc(), col("pay_count").desc())
# top_main_products = product_metrics.withColumn(
#     "main_rank", row_number().over(window_top_main)
# ).filter(col("main_rank") <= 10) \
#     .select("product_id", "product_name").withColumnRenamed("product_id", "main_product_id")
#
# # 3. 提取用户行为，分析主商品与关联商品的同时行为
# # 3.1 按用户分组，收集包含行为时间的商品列表
# user_behavior = recent_df.groupBy("user_id") \
#     .agg(
#     collect_list(
#         struct("product_id", "visit_time", "collect_time", "pay_time")
#     ).alias("product_behavior_list")
# )
#
# # 3.2 拆解用户行为，匹配主商品与关联商品
# main_product_behavior = user_behavior.select(
#     col("user_id"),
#     col("product_behavior_list"),  # 关键：保留原始商品行为列表
#     explode(col("product_behavior_list")).alias("main_product_behavior")
# ).join(
#     top_main_products,
#     col("main_product_behavior.product_id") == col("main_product_id"),
#     "inner"
# )
# # 3.3 提取关联商品行为（排除主商品自身）
# related_pairs = main_product_behavior.select(
#     col("user_id"),
#     col("main_product_id"),
#     col("main_product_behavior").alias("main_behavior"),
#     explode(col("product_behavior_list")).alias("related_behavior")
# ).filter(
#     col("related_behavior.product_id") != col("main_product_id")
# )
#
# # 4. 计算同时行为指标（访问、收藏加购、支付）
# related_metrics = related_pairs.groupBy(
#     col("main_product_id"),
#     col("related_behavior.product_id").alias("related_product_id")
# ).agg(
#     # 同时访问次数
#     count(when(
#         col("main_behavior.visit_time").isNotNull() &
#         col("related_behavior.visit_time").isNotNull(),
#         1
#     )).alias("co_visit_count"),
#
#     # 同时收藏加购次数
#     count(when(
#         col("main_behavior.collect_time").isNotNull() &
#         col("related_behavior.collect_time").isNotNull(),
#         1
#     )).alias("co_collect_count"),
#
#     # 同时段支付次数
#     count(when(
#         col("main_behavior.pay_time").isNotNull() &
#         col("related_behavior.pay_time").isNotNull(),
#         1
#     )).alias("co_pay_count")
# )
#
# # 5. 筛选每个主商品的TOP30关联宝贝（按同时支付次数排序）
# window_top_related = Window.partitionBy("main_product_id").orderBy(col("co_pay_count").desc())
# top30_related = related_metrics.withColumn(
#     "related_rank", row_number().over(window_top_related)
# ).filter(col("related_rank") <= 30) \
#     .select(
#     col("main_product_id"),
#     col("related_product_id"),
#     col("co_visit_count"),
#     col("co_collect_count"),
#     col("co_pay_count"),
#     col("related_rank")
# )
#
# # 关联主商品名称，便于展示
# result = top30_related.join(
#     top_main_products, on="main_product_id", how="inner"
# ).select(
#     "main_product_id",
#     "product_name",
#     "related_product_id",
#     "co_visit_count",
#     "co_collect_count",
#     "co_pay_count",
#     "related_rank"
# )
#
# # 展示结果
# print("近7天引流款/热销款主商品的TOP30关联宝贝：")
# result.show(10, truncate=False)
#
# # 保存结果（支持后续看板展示）
# result.write.mode("overwrite").csv("top30_related_products")
# print("结果已保存至 top30_related_products 目录")
#
# spark.stop()