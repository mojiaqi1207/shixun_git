# # ads_product.py
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
#
#
# import random
# from faker import Faker
# import pandas as pd
# import os
# os.environ["PYSPARK_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\Anaconda\envs\offline_pyspark_con\python.exe"
# spark = SparkSession.builder \
#     .appName("ads_product_efficiency_and_zone") \
#     .master("local[*]") \
#     .config("spark.sql.shuffle.partitions", 4) \
#     .getOrCreate()
#
# fake = Faker("zh_CN")
# random.seed(42)
#
# # ------------------------------------------------------------------
# # 1. 生成 100 条模拟 dwd 明细数据（商品粒度，1 天）
# # ------------------------------------------------------------------
# def mock_dwd() -> "DataFrame":
#     """
#     字段含义（一行代表一个商品在某个用户某次会话中的行为）
#     dt: 日期
#     user_id: 用户
#     sku_id: 商品
#     leaf_cate_id: 叶子类目
#     price: 原价
#     visit_time: 停留时长
#     is_bounce: 是否跳出
#     is_fav: 是否收藏
#     cart_qty: 加购件数
#     order_qty: 下单件数
#     order_amt: 下单金额
#     pay_qty: 支付件数
#     pay_amt: 支付金额
#     is_new_pay: 是否支付新客
#     is_jhs: 是否聚划算
#     refund_amt: 退款金额
#     """
#     rows = []
#     for _ in range(100000):
#         sku_id = f"sku_{random.randint(1, 20):03d}"
#         price = round(random.uniform(10, 1000), 2)
#         leaf_cate_id = f"cat_{random.randint(1, 5):02d}"
#         user_id = fake.uuid4()
#         visit_time = random.randint(5, 300)
#         is_bounce = random.choice([0, 1])
#         is_fav = random.choice([0, 1])
#         cart_qty = random.choice([0, 1, 2, 3])
#         order_qty = random.choice([0, 1, 2])
#         order_amt = round(order_qty * price * random.uniform(0.8, 1.0), 2)
#         pay_qty = order_qty if random.random() < 0.8 else 0
#         pay_amt = round(pay_qty * price * random.uniform(0.95, 1.0), 2)
#         is_new_pay = random.choice([0, 1])
#         is_jhs = random.choice([0, 1])
#         refund_amt = round(pay_amt * random.choice([0, 0, 0.1]), 2)
#
#         rows.append((
#             "2025-08-08", sku_id, leaf_cate_id, price, user_id,
#             visit_time, is_bounce, is_fav, cart_qty,
#             order_qty, order_amt, pay_qty, pay_amt,
#             is_new_pay, is_jhs, refund_amt
#         ))
#
#     schema = ["dt", "sku_id", "leaf_cate_id", "price", "user_id",
#               "visit_time", "is_bounce", "is_fav", "cart_qty",
#               "order_qty", "order_amt", "pay_qty", "pay_amt",
#               "is_new_pay", "is_jhs", "refund_amt"]
#
#     pdf = pd.DataFrame(rows, columns=schema)
#     return spark.createDataFrame(pdf)
#
# dwd = mock_dwd()
# dwd.createOrReplaceTempView("dwd_product_detail")
#
# # ------------------------------------------------------------------
# # 2. 商品效率监控 ads_product_efficiency_mon
# # ------------------------------------------------------------------
# sql_eff = """
# WITH base AS (
#     SELECT
#         dt,
#         sku_id,
#         leaf_cate_id,
#         price,
#         user_id,
#         CASE WHEN visit_time > 0 THEN 1 ELSE 0 END AS has_visit,
#         visit_time,
#         is_bounce,
#         is_fav,
#         cart_qty,
#         order_qty,
#         order_amt,
#         pay_qty,
#         pay_amt,
#         is_new_pay,
#         is_jhs,
#         refund_amt
#     FROM dwd_product_detail
# ),
# agg AS (
#     SELECT
#         dt,
#         sku_id,
#         leaf_cate_id,
#         -- 商品访客数
#         COUNT(DISTINCT user_id) AS uv,
#         -- 商品浏览量
#         SUM(has_visit) AS pv,
#         -- 有访问商品数（冗余 1）
#         1 AS has_visit_sku_flag,
#         -- 商品平均停留时长
#         SUM(visit_time) / COUNT(DISTINCT user_id) AS avg_stay_sec,
#         -- 商品详情页跳出率
#         SUM(is_bounce) / COUNT(DISTINCT user_id) AS bounce_rate,
#         -- 商品收藏人数
#         COUNT(DISTINCT CASE WHEN is_fav = 1 THEN user_id END) AS fav_users,
#         -- 商品加购件数
#         SUM(cart_qty) AS cart_qty,
#         -- 商品加购人数
#         COUNT(DISTINCT CASE WHEN cart_qty > 0 THEN user_id END) AS cart_users,
#         -- 下单买家数
#         COUNT(DISTINCT CASE WHEN order_qty > 0 THEN user_id END) AS order_buyers,
#         -- 下单件数
#         SUM(order_qty) AS order_qty,
#         -- 下单金额
#         SUM(order_amt) AS order_amt,
#         -- 支付买家数
#         COUNT(DISTINCT CASE WHEN pay_qty > 0 THEN user_id END) AS pay_buyers,
#         -- 支付件数
#         SUM(pay_qty) AS pay_qty,
#         -- 支付金额
#         SUM(pay_amt) AS pay_amt,
#         -- 有支付商品数（冗余 1）
#         CASE WHEN SUM(pay_qty) > 0 THEN 1 ELSE 0 END AS has_pay_sku_flag,
#         -- 支付新买家数
#         COUNT(DISTINCT CASE WHEN pay_qty > 0 AND is_new_pay = 1 THEN user_id END) AS new_pay_buyers,
#         -- 支付老买家数
#         COUNT(DISTINCT CASE WHEN pay_qty > 0 AND is_new_pay = 0 THEN user_id END) AS old_pay_buyers,
#         -- 老买家支付金额（仅老客支付金额）
#         SUM(CASE WHEN pay_qty > 0 AND is_new_pay = 0 THEN pay_amt ELSE 0 END) AS old_pay_amt,
#         -- 聚划算支付金额
#         SUM(CASE WHEN is_jhs = 1 THEN pay_amt ELSE 0 END) AS jhs_pay_amt,
#         -- 退款金额
#         SUM(refund_amt) AS refund_amt
#     FROM base
#     GROUP BY dt, sku_id, leaf_cate_id
# )
# SELECT
#     *,
#     -- 访问收藏转化率
#     ROUND(fav_users / uv, 4) AS fav_rate,
#     -- 访问加购转化率
#     ROUND(cart_users / uv, 4) AS cart_rate,
#     -- 下单转化率
#     ROUND(order_buyers / uv, 4) AS order_rate,
#     -- 支付转化率
#     ROUND(pay_buyers / uv, 4) AS pay_rate,
#     -- 客单价
#     ROUND(pay_amt / pay_buyers, 2) AS avg_price,
#     -- 访客平均价值
#     ROUND(pay_amt / uv, 2) AS uv_value
# FROM agg
# """
#
# ads_eff = spark.sql(sql_eff)
# ads_eff.createOrReplaceTempView("ads_product_efficiency_mon")
#
# # 落盘（parquet / hive 表均可）
# ads_eff.write.mode("overwrite").csv("/tmp/ads_product_efficiency_mon")
# print(">>> ads_product_efficiency_mon 已生成")
# ads_eff.orderBy("sku_id").show(truncate=False)
#
# # ------------------------------------------------------------------
# # 3. 商品区间分析 ads_product_zone_analysis
# #    区间维度：价格带 / 支付件数 / 支付金额
# # ------------------------------------------------------------------
# # 定义区间
# zone_map = {
#     "price_zone": [(0, 50), (51, 100), (101, 200), (201, 999999)],
#     "pay_qty_zone": [(0, 0), (1, 1), (2, 5), (6, 999999)],
#     "pay_amt_zone": [(0, 100), (101, 500), (501, 1000), (1001, 999999)]
# }
#
# def build_zone_df(df, zone_col, zone_list):
#     """
#     将数值列划分区间并生成 zone_name
#     """
#     case_expr = F
#     for idx, (low, high) in enumerate(zone_list):
#         case_expr = case_expr.when(
#             (F.col(zone_col) >= low) & (F.col(zone_col) <= high),
#             f"{low}-{high}"
#         )
#     return df.withColumn(f"{zone_col}_zone", case_expr)
#
# # 选择支付金额区间举例
# zone_df = build_zone_df(ads_eff, "pay_amt", zone_map["pay_amt_zone"])
# zone_df.createOrReplaceTempView("zone_tmp")
#
# sql_zone = """
# SELECT
#     dt,
#     pay_amt_zone,
#     leaf_cate_id,
#     -- 动销商品数
#     COUNT(DISTINCT sku_id) AS active_sku_cnt,
#     -- 支付金额
#     SUM(pay_amt) AS pay_amt,
#     -- 支付件数
#     SUM(pay_qty) AS pay_qty,
#     -- 件单价
#     ROUND(SUM(pay_amt) / SUM(pay_qty), 2) AS avg_price_per_qty
# FROM zone_tmp
# GROUP BY dt, pay_amt_zone, leaf_cate_id
# """
#
# ads_zone = spark.sql(sql_zone)
# ads_zone.write.mode("overwrite").csv("/tmp/ads_product_zone_analysis")
# print(">>> ads_product_zone_analysis 已生成")
# ads_zone.orderBy("leaf_cate_id", "pay_amt_zone").show(truncate=False)
#
# spark.stop()