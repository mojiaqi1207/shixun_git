
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWD_Product_Data") \
    .master("local[*]") \
    .getOrCreate()


# 读取ODS层数据
ods_data = spark.read.parquet("data/ods/product_ods")

# 计算支付转化率（支付买家数/商品访客数）
dwd_data = ods_data.withColumn(
    "支付转化率",
    round(col("支付买家数") / col("访客数"), 4)
).select(
    "商品ID", "商品名称", "商品分类", "SKU信息", "价格",
    "访客数", "支付买家数", "支付件数", "销售额",
    "库存数量", "流量来源", "搜索词", "日期",
    "价格力星级", "商品力评分", "支付转化率", "load_time"
)

# 保存到DWD层
dwd_data.write.mode("overwrite").partitionBy("商品分类", "日期").parquet("data/dwd/product_dwd")

spark.stop()