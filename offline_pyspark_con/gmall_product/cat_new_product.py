# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum, count, avg, max, min, expr, when  # 新增导入when
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("TmallNewProductsAnalysis") \
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
# # 读取CSV数据（请替换为实际文件路径）
# df = spark.read \
#     .format("csv") \
#     .schema(schema) \
#     .option("header", "true") \
#     .option("dateFormat", "yyyy-MM-dd") \
#     .load("new_products_data.csv")
#
# # 筛选天猫新品（is_tmall_new=1）
# tmall_new_products = df.filter(col("is_tmall_new") == 1)
#
# # 定义时间范围（根据监控数据的统计周期调整）
# start_date = "2024-06-21"
# end_date = "2024-07-20"
#
# # 筛选指定周期内的上新商品
# period_products = tmall_new_products.filter(
#     col("putaway_date").between(start_date, end_date)
# )
#
# # 汇总核心指标
# summary_stats = period_products.agg(
#     count("product_id").alias("上新商品数"),
#     sum("sales").alias("累计销售件数"),
#     sum("pay_amount").alias("累计支付金额"),
#     avg("price").alias("平均单价"),
#     max("pay_amount").alias("最高单品销售额"),
#     min("pay_amount").alias("最低单品销售额"),
#     count(when(col("sales") > 0, True)).alias("有销量商品数")  # 此处now可正常使用
# )
#
# # 展示结果
# summary_stats.show()
#
# # 停止SparkSession
# spark.stop()