

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
from faker import Faker
import os
os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GenerateMockData") \
    .master("local[*]") \
    .getOrCreate()

# 生成模拟数据
fake = Faker("zh_CN")
num_records = 1000000  # 100万条数据


# 定义数据结构
schema = StructType([
    StructField("商品ID", StringType(), True),
    StructField("商品名称", StringType(), True),
    StructField("商品分类", StringType(), True),
    StructField("SKU信息", StringType(), True),
    StructField("价格", DoubleType(), True),
    StructField("访客数", IntegerType(), True),
    StructField("支付买家数", IntegerType(), True),
    StructField("支付件数", IntegerType(), True),
    StructField("销售额", DoubleType(), True),
    StructField("库存数量", IntegerType(), True),
    StructField("流量来源", StringType(), True),
    StructField("搜索词", StringType(), True),
    StructField("日期", StringType(), True),
    StructField("价格力星级", IntegerType(), True),
    StructField("商品力评分", DoubleType(), True)
])

# 生成模拟数据
data = []
categories = ["食品", "服装", "电子产品", "家居用品", "美妆"]
traffic_sources = ["效果广告", "站外广告", "内容广告", "手淘搜索", "购物车"]
search_terms = ["轩妈家", "零食", "连衣裙", "手机", "家具", "口红"]

for _ in range(num_records):
    product_id = fake.uuid4()
    product_name = fake.word() + "商品"
    category = random.choice(categories)
    sku = f"{random.choice(['红色', '蓝色', '黑色'])}"
    price = round(random.uniform(10, 1000), 2)
    visitors = random.randint(10, 10000)
    payers = random.randint(1, visitors)
    pay_count = random.randint(1, payers * 5)
    sales = round(pay_count * price, 2)
    stock = random.randint(0, 1000)
    traffic_source = random.choice(traffic_sources)
    search_term = random.choice(search_terms)
    date = fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d")
    price_strength = random.randint(1, 5)
    product_strength = round(random.uniform(3, 5), 1)

    data.append((product_id, product_name, category, sku, price, visitors, payers,
                 pay_count, sales, stock, traffic_source, search_term, date,
                 price_strength, product_strength))

# 创建DataFrame并保存到本地
df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").parquet("data/mock_data")

spark.stop()