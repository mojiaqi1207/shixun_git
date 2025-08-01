from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import random
from datetime import datetime, timedelta
import os
os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# 初始化SparkSession
spark = SparkSession.builder \
    .appName("商品主题连带分析模拟数据生成") \
    .master("local[*]") \
    .getOrCreate()

# 定义数据结构，包含用户及商品行为核心字段
schema = StructType([
    StructField("user_id", StringType(), nullable=False),  # 用户ID
    StructField("product_id", StringType(), nullable=False),  # 商品ID
    StructField("product_name", StringType(), nullable=False),  # 商品名称
    StructField("category", StringType(), nullable=False),  # 商品类目
    StructField("visit_time", TimestampType(), nullable=True),  # 访问时间
    StructField("collect_time", TimestampType(), nullable=True),  # 收藏加购时间
    StructField("pay_time", TimestampType(), nullable=True),  # 支付时间
    StructField("price", DoubleType(), nullable=False),  # 商品价格
    StructField("pay_amount", DoubleType(), nullable=True)  # 支付金额
])

# 生成模拟数据，覆盖近7天的用户行为
def generate_mock_data():
    data = []
    # 商品池：包含引流款、热销款主商品及其他关联商品
    main_products = [f"P{100+i}" for i in range(50)]  # 主商品（含引流款、热销款）
    other_products = [f"P{200+i}" for i in range(150)]  # 其他关联商品
    all_products = main_products + other_products

    # 时间范围：近7天
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)

    # 生成2000条用户行为数据
    for _ in range(2000):
        user_id = f"U{random.randint(1000, 9999)}"
        product_id = random.choice(all_products)
        product_name = f"{random.choice(['男装', '女装', '数码', '美妆'])}_商品{product_id[-3:]}"
        category = product_name.split("_")[0]

        # 随机生成近7天内的行为时间
        time_offset = timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
        visit_time = start_time + time_offset if random.random() < 0.8 else None  # 80%概率有访问
        # 访问后可能产生收藏加购或支付行为
        collect_time = visit_time + timedelta(minutes=random.randint(1, 30)) if visit_time and random.random() < 0.3 else None
        pay_time = visit_time + timedelta(minutes=random.randint(5, 60)) if visit_time and random.random() < 0.2 else None

        price = round(random.uniform(19.9, 1999.9), 2)
        pay_amount = round(price * (1 + random.uniform(-0.05, 0.05)), 2) if pay_time else None

        data.append((
            user_id, product_id, product_name, category,
            visit_time, collect_time, pay_time, price, pay_amount
        ))
    return spark.createDataFrame(data, schema)

# 生成并保存模拟数据
mock_df = generate_mock_data()
mock_df.write.csv("mock_related_products_data", header=True, mode="overwrite")
print("模拟数据生成完成，路径：mock_related_products_data")

spark.stop()