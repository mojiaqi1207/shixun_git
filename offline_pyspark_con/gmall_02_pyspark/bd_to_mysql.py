
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
import os
os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"

# 初始化SparkSession，增加MySQL连接依赖
spark = SparkSession.builder \
    .appName("ADS_Product_Ranking_To_MySQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", "mysql-connector-java-8.0.28.jar")  \
    .getOrCreate()

# 读取DWS层数据
dws_data = spark.read.parquet("data/dws/product_dws")

# 1. 生成商品销售额排行榜（按分类和日期）
rank_window = Window.partitionBy("商品分类", "日期").orderBy(col("总销售额").desc())
sales_ranking = dws_data.withColumn(
    "销售额排名",
    row_number().over(rank_window)
).select(
    "商品ID", "商品名称", "商品分类", "日期",
    "总销售额", "销售额排名", "总支付件数", "总访客数"
)

# 2. 生成价格力商品排行
price_strength_ranking = dws_data.orderBy(
    col("平均价格力星级").desc(),
    col("平均商品力评分").desc()
).select(
    "商品ID", "商品名称", "平均价格力星级",
    "平均商品力评分", "总销售额", "日期"
).limit(1000)

# MySQL连接配置
mysql_url = "jdbc:mysql://cdh03:3306/gmall_pyspark_02?useSSL=false&serverTimezone=UTC"  # 数据库名对应项目名称
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 写入MySQL（表名对应看板需求）
sales_ranking.write.mode("overwrite").jdbc(
    url=mysql_url,
    table="商品销售额排行",  # 对应文档中销售额排名展示需求
    properties=mysql_properties
)

price_strength_ranking.write.mode("overwrite").jdbc(
    url=mysql_url,
    table="价格力商品排行",  # 对应文档中价格力商品排行需求
    properties=mysql_properties
)

spark.stop()
