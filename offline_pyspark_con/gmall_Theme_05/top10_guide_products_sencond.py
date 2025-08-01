# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, countDistinct, count, sum, when, collect_list, struct,
#     row_number, date_sub, current_date, lag, unix_timestamp
# )
# from pyspark.sql.window import Window
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("商品主题连带效果分析") \
#     .master("local[*]") \
#     .getOrCreate()
#
# # 工单编号：大数据-电商数仓-05-商品主题连带分析看板
#
# # 1. 读取近7天数据（包含用户访问、收藏、支付行为）
# analysis_start_date = date_sub(current_date(), 7)
# df = spark.read.csv("mock_related_products_data", header=True, inferSchema=True) \
#     .filter(
#     (col("user_id").isNotNull()) &
#     (col("product_id").isNotNull()) &
#     (col("visit_time").isNotNull()) &
#     (col("visit_time") >= analysis_start_date)
# )
#
# # 2. 推断详情页引导关系（通过用户访问序列）
# # 2.1 按用户分组并按访问时间排序，获取上一次访问的商品（引导来源）
# user_visit_window = Window.partitionBy("user_id").orderBy("visit_time")
# user_visit_sequence = df.select(
#     "user_id", "product_id", "product_name",
#     "visit_time", "collect_time", "pay_time", "pay_amount"
# ).withColumn(
#     "prev_product_id",  # 上一访问商品（主商品，引导来源）
#     lag("product_id").over(user_visit_window)
# ).withColumn(
#     "prev_visit_time",  # 上一访问时间
#     lag("visit_time").over(user_visit_window)
# )
#
# # 2.2 定义有效引导：两次访问间隔≤30分钟（符合详情页跳转场景）
# valid_guide = user_visit_sequence.filter(
#     col("prev_product_id").isNotNull() &
#     (col("product_id") != col("prev_product_id")) &
#     (unix_timestamp("visit_time") - unix_timestamp("prev_visit_time") <= 60*60)  # 60分钟
# ).withColumnRenamed("prev_product_id", "main_product_id") \
#     .withColumnRenamed("product_id", "related_product_id") \
#     .withColumnRenamed("product_name", "related_product_name")
#
# # 3. 计算主商品引导能力指标（核心指标）
# main_product_metrics = valid_guide.groupBy("main_product_id") \
#     .agg(
#     countDistinct("user_id").alias("guide_uv"),  # 引导访客数（排序依据）
#     count(when(col("collect_time").isNotNull(), 1)).alias("guide_collect_cnt"),  # 引导加购数
#     count(when(col("pay_time").isNotNull(), 1)).alias("guide_pay_cnt"),  # 引导支付数
#     sum(when(col("pay_time").isNotNull(), col("pay_amount")).otherwise(0)).alias("guide_pay_amt")  # 引导支付金额
# )
#
# # 4. 关联主商品名称（用于展示）
# main_product_names = df.select("product_id", "product_name").distinct() \
#     .withColumnRenamed("product_id", "main_product_id") \
#     .withColumnRenamed("product_name", "main_product_name")
# main_product_metrics = main_product_metrics.join(main_product_names, on="main_product_id", how="inner")
#
# # 5. 筛选TOP10引导能力最强商品（按引导访客数排序）
# top10_guide_window = Window.orderBy(col("guide_uv").desc())
# top10_guide_products = main_product_metrics.withColumn(
#     "guide_rank", row_number().over(top10_guide_window)
# ).filter(col("guide_rank") <= 10)  # 默认展示TOP10
#
# # 6. 生成关联宝贝明细清单
# related_details = valid_guide.join(
#     top10_guide_products.select("main_product_id"), on="main_product_id", how="inner"
# ).groupBy("main_product_id", "related_product_id", "related_product_name") \
#     .agg(
#     countDistinct("user_id").alias("related_guide_uv"),  # 被引导访客数
#     count(when(col("collect_time").isNotNull(), 1)).alias("related_collect_cnt"),
#     count(when(col("pay_time").isNotNull(), 1)).alias("related_pay_cnt")
# )
#
# # 7. 整合结果（主商品+关联明细）
# final_result = top10_guide_products.join(related_details, on="main_product_id", how="left") \
#     .select(
#     "main_product_id", "main_product_name", "guide_uv", "guide_rank",
#     "related_product_id", "related_product_name",
#     "related_guide_uv", "related_collect_cnt", "related_pay_cnt"
# )
#
# # 8. 结果输出（支持看板展示）
# print("TOP10引导能力最强商品：")
# top10_guide_products.select("main_product_id", "main_product_name", "guide_uv", "guide_rank").show(10)
#
# print("关联宝贝明细清单：")
# final_result.show(20, truncate=False)
#
# # 保存结果（符合验收标准中的数据存储要求）
# top10_guide_products.write.mode("overwrite").csv("top10_guide_products", header=True)
# # final_result.write.mode("overwrite").parquet("related_products_details")
# print("结果已保存至 top10_guide_products 目录")
#
# spark.stop()