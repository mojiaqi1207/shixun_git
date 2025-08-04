# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum, count, avg, max, min, datediff, current_date, when, row_number
# from pyspark.sql.window import Window
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("NewProductsAnalysis") \
#     .getOrCreate()
#
# # 先不指定schema，自动推断数据结构
# print("开始读取数据...")
# try:
#     df = spark.read \
#         .format("csv") \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .load("new_products_data.csv")
#     print("数据读取完成。")
#     print("数据基本信息：")
#     df.printSchema()
#     rows = df.count()
#     if rows == 0:
#         print("读取到的数据行数为0，请检查数据文件内容是否为空。")
#     else:
#         print(f"数据行数: {rows}")
# except Exception as e:
#     print(f"数据读取过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 查看is_tmall_new列的唯一值
# print("查看is_tmall_new列的唯一值...")
# df.select("is_tmall_new").distinct().show()
#
# # 暂时注释掉时间筛选条件
# print("开始筛选天猫新品...")
# try:
#     valid_products = df.filter(col("is_tmall_new") == 1)
#     print("筛选完成。")
#     valid_rows = valid_products.count()
#     if valid_rows == 0:
#         print("经过筛选后有效数据行数为0，请检查筛选条件是否合理。")
#     else:
#         print(f"有效数据行数: {valid_rows}")
# except Exception as e:
#     print(f"筛选过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 1. 按标签和类目聚合的销售数据（用于类目对比图）
# print("开始按标签和类目聚合销售数据...")
# try:
#     category_tag_stats = valid_products.groupBy("tag", "category") \
#         .agg(
#         count("product_id").alias("商品数量"),
#         sum("sales").alias("总销量"),
#         sum("pay_amount").alias("总支付金额"),
#         avg("pay_amount").alias("平均单品销售额")
#     ) \
#         .orderBy("tag", "category")# 按标签和类目排序
#     print("聚合完成。")
# except Exception as e:
#     print(f"聚合过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 保存类目对比数据
# print("开始保存类目对比数据...")
# try:
#     category_tag_stats.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save("category_tag_analysis")
#     print("类目对比数据保存完成。")
# except Exception as e:
#     print(f"保存类目对比数据过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 2. 各标签Top10单品数据（用于单品排行榜）
# print("开始获取各标签Top10单品数据...")
# try:
#     window = Window.partitionBy("tag").orderBy(col("pay_amount").desc())
#     top_products = valid_products.withColumn(
#         "rank", row_number().over(window)
#     ).filter(col("rank") <= 10) \
#         .select(
#         "tag", "product_id", "title", "category",
#         "pay_amount", "sales", "price", "putaway_date"
#     )
#     print("获取完成。")
# except Exception as e:
#     print(f"获取Top10单品数据过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 保存Top10单品数据
# print("开始保存Top10单品数据...")
# try:
#     top_products.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save("top10_products_by_tag")
#     print("Top10单品数据保存完成。")
# except Exception as e:
#     print(f"保存Top10单品数据过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 3. 价格区间分布数据（用于价格分布直方图）
# print("开始生成价格区间分布数据...")
# try:
#     price_bins = valid_products.withColumn(
#         "price_range",
#         when(col("price") <= 1000, "0-1000")
#         .when(col("price") <= 3000, "1001-3000")
#         .when(col("price") <= 5000, "3001-5000")
#         .when(col("price") <= 10000, "5001-10000")
#         .otherwise("10001+")
#     ).groupBy("tag", "price_range") \
#         .agg(
#         count("product_id").alias("商品数量"),
#         sum("pay_amount").alias("总支付金额")
#     ) \
#         .orderBy("tag", "price_range")
#     print("生成完成。")
# except Exception as e:
#     print(f"生成价格区间分布数据过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 保存价格分布数据
# print("开始保存价格分布数据...")
# try:
#     price_bins.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save("price_distribution_analysis")
#     print("价格分布数据保存完成。")
# except Exception as e:
#     print(f"保存价格分布数据过程中出现异常: {e}")
#     spark.stop()
#     raise
#
# # 展示结果示例
# print("类目标签对比数据:")
# category_tag_stats.show(10)
# print("Top10单品数据:")
# top_products.show(10)
# print("价格区间分布数据:")
# price_bins.show(10)
#
# spark.stop()