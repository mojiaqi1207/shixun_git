# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, year, month, dayofmonth, date_format, sum
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("NewProductsAnnualReview") \
#     .getOrCreate()
#
# # 定义数据结构
# schema = StructType([
#     StructField("product_id", StringType(), True),
#     StructField("category", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("putaway_date", DateType(), True),
#     StructField("price", DoubleType(), True),
#     StructField("sales", IntegerType(), True),
#     StructField("pay_amount", DoubleType(), True),
#     StructField("tag", StringType(), True),
#     StructField("is_tmall_new", IntegerType(), True)
# ])
#
# # 读取CSV数据（替换为实际文件路径）
# df = spark.read \
#     .format("csv") \
#     .schema(schema) \
#     .option("header", "true") \
#     .option("dateFormat", "yyyy-MM-dd") \
#     .load("new_products_data.csv")
#
# # 筛选天猫新品（is_tmall_new=1）并提取日期维度
# tmall_products = df.filter(col("is_tmall_new") == 1) \
#     .select(
#     col("putaway_date"),
#     col("pay_amount"),
#     year("putaway_date").alias("year"),
#     month("putaway_date").alias("month"),
#     dayofmonth("putaway_date").alias("day"),
#     date_format("putaway_date", "EEEE").alias("weekday"),  # 获取星期几
#     date_format("putaway_date", "MM-dd").alias("month_day")  # 月-日格式
# )
#
# # 按日期聚合支付金额（用于热力图权重）
# daily_summary = tmall_products.groupBy(
#     "year", "month", "day", "month_day", "weekday"
# ).agg(
#     sum("pay_amount").alias("total_pay_amount")
# ).orderBy("year", "month", "day")
#
# # 补充全年所有日期（处理无新品日期）
# from pyspark.sql.functions import expr
# date_range = spark.sql("""
#     SELECT
#         explode(sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)) as putaway_date
# """)
#
# full_dates = date_range.select(
#     year("putaway_date").alias("year"),
#     month("putaway_date").alias("month"),
#     dayofmonth("putaway_date").alias("day"),
#     date_format("putaway_date", "MM-dd").alias("month_day"),
#     date_format("putaway_date", "EEEE").alias("weekday")
# )
#
# # 左连接获取完整日期数据（无新品日期支付金额为0）
# heatmap_data = full_dates.join(
#     daily_summary,
#     ["year", "month", "day", "month_day", "weekday"],
#     "left"
# ).fillna(0, subset=["total_pay_amount"])
#
# # 计算热力等级（1-5级，用于可视化深浅）
# max_pay = heatmap_data.agg({"total_pay_amount": "max"}).collect()[0][0]
# heatmap_data = heatmap_data.withColumn(
#     "heat_level",
#     expr(f"case when total_pay_amount = 0 then 0 else floor((total_pay_amount / {max_pay}) * 5) + 1 end")
# )
#
# # 展示结果（按月份和日期排序）
# heatmap_data.orderBy("month", "day").show(30)
#
# # 保存结果（可用于Excel热力图或BI工具可视化）
# heatmap_data.write \
#     .format("csv") \
#     .option("header", "true") \
#     .mode("overwrite") \
#     .save("new_products_heatmap_data")
#
# spark.stop()