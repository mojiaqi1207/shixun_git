
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
import os
os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ADS_Product_Ranking") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
# 读取DWS层数据
dws_data = spark.read.parquet("data/dws/product_dws")

# 1. 商品销售额排行榜（按分类和日期）
rank_window = Window.partitionBy("商品分类", "日期").orderBy(col("总销售额").desc())
sales_ranking = dws_data.withColumn(
    "销售额排名",
    row_number().over(rank_window)
).select(
    "商品ID", "商品名称", "商品分类", "日期",
    "总销售额", "销售额排名", "总支付件数", "总访客数"
)


# 2. 价格力商品排行
price_strength_ranking = dws_data.orderBy(
    col("平均价格力星级").desc(),
    col("平均商品力评分").desc()
).select(
    "商品ID", "商品名称", "平均价格力星级",
    "平均商品力评分", "总销售额", "日期"
).limit(1000)

# 保存到ADS层
sales_ranking.write.mode("overwrite").csv("data/ads/product_sales_ranking")
price_strength_ranking.write.mode("overwrite").csv("data/ads/price_strength_ranking")

spark.stop()