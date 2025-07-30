# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum, avg, max, min, datediff, current_date, when, rank
# from pyspark.sql.window import Window
# import random
# from datetime import datetime, timedelta
# import pandas as pd
# from pyspark.sql.functions import col, count, countDistinct, sum, avg, max, min, datediff, current_date, when, rank
# # 初始化SparkSession
# def init_spark():
#     spark = SparkSession.builder \
#         .appName("E-Commerce New Product Tracking") \
#         .master("local[*]") \
#         .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
#         .config("spark.python.worker.connectionTimeout", "600s") \
#         .config("spark.network.timeout", "800s") \
#         .config("spark.sql.adaptive.enabled", "false") \
#         .config("spark.python.worker.reuse", "false") \
#         .getOrCreate()
#
#     return spark
#
# # 生成模拟数据
#
#
#
# def generate_mock_data(spark, num_products=128, start_date=None, end_date=None):
#     if not start_date:
#         end_date = datetime.now()
#         start_date = end_date - timedelta(days=15)  # 默认近15天
#
#     # 商品分类
#     categories = ["电子产品", "服装鞋帽", "家居用品", "美妆个护", "食品饮料", "图书音像", "运动户外"]
#
#     # 生成商品数据
#     products_data = []
#     for i in range(1, num_products + 1):
#         product_id = f"P{20230000 + i}"
#         category = random.choice(categories)
#         launch_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
#         price = round(random.uniform(19.9, 1999.9), 2)
#         brand = f"品牌{random.randint(1, 20)}"
#
#         products_data.append({
#             "product_id": product_id,
#             "product_name": f"{category}新品{i}",
#             "category": category,
#             "launch_date": launch_date.strftime("%Y-%m-%d"),
#             "price": price,
#             "brand": brand
#         })
#
#     # 生成销售数据
#     sales_data = []
#     for product in products_data:
#         product_id = product["product_id"]
#         launch_date = datetime.strptime(product["launch_date"], "%Y-%m-%d")
#         days_since_launch = (end_date - launch_date).days
#
#         # 为每个商品生成每日销售数据
#         for day in range(days_since_launch + 1):
#             sale_date = (launch_date + timedelta(days=day)).strftime("%Y-%m-%d")
#             if sale_date > end_date.strftime("%Y-%m-%d"):
#                 continue
#
#             # 新商品销量通常有增长趋势
#             base_sales = random.randint(5, 50)
#             sales_qty = base_sales + int(base_sales * (day / days_since_launch) * 2) if days_since_launch > 0 else base_sales
#             sales_amount = round(sales_qty * product["price"] * random.uniform(0.9, 1.1), 2)  # 考虑一些价格波动
#             clicks = sales_qty * random.randint(10, 30)  # 点击量通常是销量的10-30倍
#             conversions = round(sales_qty / clicks * 100, 2) if clicks > 0 else 0
#
#             sales_data.append({
#                 "product_id": product_id,
#                 "sale_date": sale_date,
#                 "sales_qty": sales_qty,
#                 "sales_amount": sales_amount,
#                 "clicks": clicks,
#                 "conversions": conversions
#             })
#
#     # 生成评价数据
#     reviews_data = []
#     for product in products_data:
#         product_id = product["product_id"]
#         launch_date = datetime.strptime(product["launch_date"], "%Y-%m-%d")
#         days_since_launch = (end_date - launch_date).days
#
#         # 随机生成10-50条评价
#         num_reviews = random.randint(10, 50)
#         for _ in range(num_reviews):
#             review_date = (launch_date + timedelta(days=random.randint(1, days_since_launch))).strftime("%Y-%m-%d") if days_since_launch > 0 else launch_date.strftime("%Y-%m-%d")
#             rating = random.randint(3, 5) if random.random() > 0.1 else random.randint(1, 2)  # 10%概率低分
#             reviews_data.append({
#                 "product_id": product_id,
#                 "review_date": review_date,
#                 "rating": rating,
#                 "review_text": f"这是关于{product['product_name']}的评价"
#             })
#
#     # 转换为Spark DataFrame
#     products_df = spark.createDataFrame(products_data)
#     sales_df = spark.createDataFrame(sales_data)
#     reviews_df = spark.createDataFrame(reviews_data)
#
#     return products_df, sales_df, reviews_df
#
# # 计算核心指标
# def calculate_core_metrics(products_df, sales_df, reviews_df):
#     # 1. 总体指标
#     total_products = products_df.count()
#
#     # 2. 销售总额
#     total_sales = sales_df.agg(sum("sales_amount").alias("total_sales")).collect()[0]["total_sales"]
#
#     # 3. 平均转化率
#     avg_conversion = sales_df.agg(avg("conversions").alias("avg_conversion")).collect()[0]["avg_conversion"]
#
#     # 4. 平均评分
#     avg_rating = reviews_df.agg(avg("rating").alias("avg_rating")).collect()[0]["avg_rating"]
#
#     print(f"核心指标概览:")
#     print(f"新增商品总数: {total_products}")
#     print(f"新品总销售额: {round(total_sales, 2)}元")
#     print(f"平均转化率: {round(avg_conversion, 2)}%")
#     print(f"平均评分: {round(avg_rating, 2)}")
#
#     return {
#         "total_products": total_products,
#         "total_sales": total_sales,
#         "avg_conversion": avg_conversion,
#         "avg_rating": avg_rating
#     }
#
# # 按分类分析新品表现
# def analyze_by_category(products_df, sales_df, reviews_df):
#     # 关联数据
#     product_sales = products_df.join(sales_df, on="product_id", how="left")
#     product_reviews = products_df.join(reviews_df, on="product_id", how="left")
#
#     # 按分类聚合销售数据
#     category_sales = product_sales.groupBy("category") \
#         .agg(
#         countDistinct("product_id").alias("product_count"),
#         sum("sales_amount").alias("total_sales"),
#         avg("conversions").alias("avg_conversion")
#     )
#
#     # 按分类聚合评价数据
#     category_ratings = product_reviews.groupBy("category") \
#         .agg(avg("rating").alias("avg_rating"))
#
#     # 合并结果
#     category_analysis = category_sales.join(category_ratings, on="category", how="left") \
#         .orderBy(col("total_sales").desc())
#
#     print("\n按分类分析:")
#     category_analysis.show(truncate=False)
#
#     return category_analysis
#
# # 分析表现最佳的新品
# def analyze_top_products(products_df, sales_df, reviews_df, top_n=10):
#     # 计算每个商品的总销售额
#     product_total_sales = sales_df.groupBy("product_id") \
#         .agg(
#         sum("sales_amount").alias("total_sales"),
#         avg("conversions").alias("avg_conversion")
#     )
#
#     # 计算每个商品的平均评分
#     product_avg_rating = reviews_df.groupBy("product_id") \
#         .agg(avg("rating").alias("avg_rating"))
#
#     # 关联商品信息
#     top_products = products_df \
#         .join(product_total_sales, on="product_id", how="left") \
#         .join(product_avg_rating, on="product_id", how="left") \
#         .orderBy(col("total_sales").desc()) \
#         .limit(top_n)
#
#     print(f"\n表现最佳的{top_n}个新品:")
#     top_products.show(truncate=False)
#
#     return top_products
#
# # 分析新品销售趋势
# def analyze_sales_trend(products_df, sales_df):
#     # 计算每日新品销售总额
#     daily_sales = sales_df.groupBy("sale_date") \
#         .agg(sum("sales_amount").alias("daily_total_sales")) \
#         .orderBy("sale_date")
#
#     print("\n每日销售趋势:")
#     daily_sales.show(truncate=False)
#
#     return daily_sales
#
# # 主函数
# def main():
#     # 初始化Spark
#     spark = init_spark()
#
#     # 生成模拟数据
#     print("生成模拟数据...")
#     products_df, sales_df, reviews_df = generate_mock_data(spark)
#
#     # 计算核心指标
#     core_metrics = calculate_core_metrics(products_df, sales_df, reviews_df)
#
#     # 按分类分析
#     category_analysis = analyze_by_category(products_df, sales_df, reviews_df)
#
#     # 分析最佳新品
#     top_products = analyze_top_products(products_df, sales_df, reviews_df)
#
#     # 分析销售趋势
#     sales_trend = analyze_sales_trend(products_df, sales_df)
#
#     # 可以将结果保存到文件或数据库
#     category_analysis.write.mode("overwrite").csv("category_analysis.csv", header=True)
#     top_products.write.mode("overwrite").csv("top_products.csv", header=True)
#     sales_trend.write.mode("overwrite").csv("sales_trend.csv", header=True)
#
#     # 停止Spark
#     spark.stop()
#
# if __name__ == "__main__":
#     main()
