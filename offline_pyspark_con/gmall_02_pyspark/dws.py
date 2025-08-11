
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, round, col  # 补充导入col函数

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWS_Product_Data") \
    .master("local[*]") \
    .getOrCreate()

# 读取DWD层数据
dwd_data = spark.read.parquet("data/dwd/product_dwd")


# 按商品和日期汇总
dws_data = dwd_data.groupBy("商品ID", "商品名称", "商品分类", "日期") \
    .agg(
    sum("访客数").alias("总访客数"),
    sum("支付买家数").alias("总支付买家数"),
    sum("支付件数").alias("总支付件数"),
    sum("销售额").alias("总销售额"),
    avg("支付转化率").alias("平均支付转化率"),
    max("库存数量").alias("当前库存"),
    avg("价格力星级").alias("平均价格力星级"),
    avg("商品力评分").alias("平均商品力评分")
) \
    .withColumn("平均支付转化率", round(col("平均支付转化率"), 4))

# 保存到DWS层
dws_data.write.mode("overwrite").partitionBy("商品分类", "日期").parquet("data/dws/product_dws")

spark.stop()