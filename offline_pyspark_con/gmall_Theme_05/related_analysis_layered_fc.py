# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, countDistinct, count, sum, collect_list, struct, row_number, date_sub, current_date
# from pyspark.sql.window import Window
# # 在文件顶部的导入部分，将原来的导入语句修改为：
# from pyspark.sql.functions import col, countDistinct, count, sum, collect_list, struct, row_number, date_sub, current_date, when
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("商品主题连带分析-分层实现") \
#     .master("local[*]") \
#     .getOrCreate()
#
# # 工单编号：大数据-电商数仓-05-商品主题连带分析看板{insert\_element\_0\_}
#
# # 1. ODS层：读取原始数据
# ods_df = spark.read.csv("mock_related_products_data", header=True, inferSchema=True)
# ods_df.createOrReplaceTempView("ods_related_products")
# print("ODS层数据示例：")
# ods_df.select("user_id", "product_id", "visit_time").show(3)
#
# # 2. DWD层：数据清洗与转换，筛选近7天有效数据
# dwd_df = ods_df.filter(
#     (col("user_id").isNotNull()) &
#     (col("product_id").isNotNull()) &
#     ((col("visit_time").isNotNull()) |
#      (col("collect_time").isNotNull()) |
#      (col("pay_time").isNotNull()))
# ).withColumn("analysis_start_date", date_sub(current_date(), 7))  # 近7天分析起点
# dwd_df.createOrReplaceTempView("dwd_related_products")
# print("\nDWD层清洗后数据：")
# dwd_df.select("product_id", "category", "analysis_start_date").show(3)
#
# # 3. DWS层：计算商品行为指标及关联关系
# # 3.1 商品基础指标（用于识别TOP主商品）
# dws_product_metrics = dwd_df.groupBy("product_id", "product_name", "category") \
#     .agg(
#     countDistinct("user_id").alias("uv"),  # 访客数（引流能力）
#     count("visit_time").alias("visit_count"),  # 访问次数
#     count("pay_time").alias("pay_count"),  # 支付次数（销量相关）
#     sum("pay_amount").alias("total_pay")  # 支付金额
# )
# dws_product_metrics.createOrReplaceTempView("dws_product_metrics")
#
# # 3.2 商品关联关系（同时访问/收藏/支付的商品对）
# # 按用户分组获取行为涉及的商品列表
# user_products = dwd_df.groupBy("user_id") \
#     .agg(collect_list(struct("product_id", "visit_time", "collect_time", "pay_time")).alias("product_actions"))
#
# # 提取商品对并计算关联指标
# from pyspark.sql.functions import explode, array_contains
# related_pairs = user_products.select(
#     col("user_id"),
#     explode(col("product_actions")).alias("main_product"),
#     explode(col("product_actions")).alias("related_product")
# ).filter(col("main_product.product_id") != col("related_product.product_id"))  # 排除自身关联
#
# dws_related = related_pairs.groupBy(
#     col("main_product.product_id").alias("main_product_id"),
#     col("related_product.product_id").alias("related_product_id")
# ).agg(
#     count("user_id").alias("co_visit_count"),  # 同时访问次数
#     count(when(
#         col("main_product.collect_time").isNotNull() & col("related_product.collect_time").isNotNull(),
#         1
#     )).alias("co_collect_count"),  # 同时收藏加购次数
#     count(when(
#         col("main_product.pay_time").isNotNull() & col("related_product.pay_time").isNotNull(),
#         1
#     )).alias("co_pay_count")  # 同时段支付次数
# )
# dws_related.createOrReplaceTempView("dws_related_products")
#
# # 4. ADS层：生成看板展示数据
# # 4.1 TOP主商品与关联TOP30宝贝（对应任务中"TOP单品与其他商品的无序关联关系"要求）{insert\_element\_1\_}
# # 筛选TOP10主商品（引流能力最强、销量最高）
# window_top_main = Window.orderBy(col("uv").desc(), col("pay_count").desc())
# top_main_products = dws_product_metrics.withColumn(
#     "main_rank", row_number().over(window_top_main)
# ).filter(col("main_rank") <= 10).select("product_id", "product_name")
#
# # 关联商品取TOP30
# window_related = Window.partitionBy("main_product_id").orderBy(col("co_pay_count").desc())
# ads_relation_insight = dws_related.join(
#     top_main_products,
#     col("main_product_id") == col("product_id"),
#     "inner"
# ).withColumn(
#     "related_rank", row_number().over(window_related)
# ).filter(col("related_rank") <= 30) \
#     .select(
#     col("main_product_id"),
#     col("product_name").alias("main_product_name"),
#     col("related_product_id"),
#     col("co_visit_count"),
#     col("co_collect_count"),
#     col("co_pay_count")
# )
# print("\nADS层关联洞察数据：")
# ads_relation_insight.show(3)
#
# # 4.2 连带效果（引导能力TOP10商品及关联明细）
# ads_guide_effect = dws_related.groupBy("main_product_id") \
#     .agg(
#     sum("co_visit_count").alias("total_guide_visits"),  # 总引导访问量
#     collect_list(struct("related_product_id", "co_visit_count")).alias("related_details")
# ) \
#     .join(dws_product_metrics.select("product_id", "product_name"),
#           col("main_product_id") == col("product_id"), "inner") \
#     .orderBy(col("total_guide_visits").desc()) \
#     .limit(10) \
#     .select("main_product_id", "product_name", "total_guide_visits", "related_details")
# print("\nADS层连带效果数据：")
# ads_guide_effect.show(3, truncate=False)
#
# # 保存结果（使用Parquet格式）
# ads_relation_insight.write.csv("ads_relation_insight", header=True, mode="overwrite")
# ads_guide_effect.write.parquet("ads_guide_effect", mode="overwrite")  # 修改为Parquet格式
# spark.stop()