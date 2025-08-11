# import pyspark
# from pyspark.sql import SparkSession
#
# # 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("ODS_Product_Data") \
#     .master("local[*]") \
#     .getOrCreate()
#
# # 读取模拟数据
# mock_data = spark.read.parquet("data/mock_data")
#
# # 数据清洗：处理空值和异常值，给中文列名添加反引号转义
# ods_data = mock_data.na.fill({
#     "商品ID": "unknown",
#     "商品名称": "unknown",
#     "库存数量": 0
# }).filter("`价格` > 0 and `访客数` >= 0")  # 关键修改处
#
#
# # 增加数据加载时间
# from pyspark.sql.functions import current_timestamp
# ods_data = ods_data.withColumn("load_time", current_timestamp())
#
# # 保存到ODS层
# ods_data.write.mode("overwrite").partitionBy("日期").parquet("data/ods/product_ods")
#
# spark.stop()