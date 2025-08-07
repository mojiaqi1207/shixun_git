# import pandas as pd
# import numpy as np
# import random
# from datetime import datetime, timedelta
# import os
#
# # 配置参数
# start_date = "2025-01-01"  # 起始日期
# end_date = "2025-01-25"    # 结束日期（与工单创建时间匹配）
# shop_ids = ["shop_001", "shop_002", "shop_003"]  # 模拟店铺ID
# categories = ["服饰", "电子产品", "食品", "家居"]  # 商品分类
# traffic_sources = ["效果广告", "站外广告", "内容广告", "手淘搜索", "购物车", "我的淘宝", "手淘推荐"]  # 流量来源（参考文档）
# time_types = ["day", "week", "month", "7days", "30days"]  # 时间维度类型
#
# # 生成日期列表
# date_list = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
#
# # 创建输出目录
# output_dir = "ods_mock_data"
# os.makedirs(output_dir, exist_ok=True)
#
# # 1. 商品基础信息表（ods_sku_base_info）
# def generate_sku_base_info():
#     data = []
#     sku_id = 10000
#     for dt in date_list:
#         for shop_id in shop_ids:
#             for cate in categories:
#                 for _ in range(5):  # 每个分类下生成5个SKU
#                     sku_id += 1
#                     data.append({
#                         "sku_id": f"sku_{sku_id}",
#                         "product_id": f"prod_{sku_id//10}",  # 10个SKU对应1个商品
#                         "shop_id": shop_id,
#                         "sku_name": f"{cate}_商品_{sku_id}",
#                         "color": random.choice(["红", "蓝", "黑", "白", "绿"]),
#                         "price": round(random.uniform(10, 500), 2),
#                         "category_id": f"cate_{categories.index(cate) + 1}",
#                         "category_name": cate,
#                         "create_time": (datetime.strptime(dt, "%Y-%m-%d") - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
#                         "dt": dt
#                     })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_sku_base_info.csv", index=False, sep="\t")
#     print("生成ods_sku_base_info完成")
#
# # 2. 商品流量来源表（ods_product_traffic_source）
# def generate_product_traffic_source():
#     data = []
#     product_ids = [f"prod_{i}" for i in range(1000, 1500)]  # 模拟商品ID
#     for dt in date_list:
#         for tt in time_types:
#             for shop_id in shop_ids:
#                 for prod_id in random.sample(product_ids, 30):  # 每天随机30个商品
#                     for source in random.sample(traffic_sources, 5):  # 每个商品随机5个流量来源
#                         visitors = random.randint(100, 5000)
#                         pay_conv = round(random.uniform(0.01, 0.3), 4)  # 支付转化率1%-30%
#                         data.append({
#                             "product_id": prod_id,
#                             "shop_id": shop_id,
#                             "traffic_source": source,
#                             "visitor_count": visitors,
#                             "pay_conversion_rate": pay_conv,
#                             "data_dt": dt,
#                             "time_type": tt,
#                             "dt": dt
#                         })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_product_traffic_source.csv", index=False, sep="\t")
#     print("生成ods_product_traffic_source完成")
#
# # 3. 商品销售明细表（ods_sku_sales_detail）
# def generate_sku_sales_detail():
#     data = []
#     sku_ids = [f"sku_{i}" for i in range(10000, 12000)]  # 模拟SKU
#     for dt in date_list:
#         for tt in time_types:
#             for sku in random.sample(sku_ids, 50):  # 每天随机50个SKU
#                 pay_count = random.randint(10, 500)
#                 price = round(random.uniform(20, 1000), 2)
#                 sales_ratio = round(random.uniform(0.05, 0.8), 4)  # 销量占比
#                 stock = random.randint(50, 1000)
#                 stock_days = round(stock / (pay_count + 1), 1)  # 可售天数
#                 data.append({
#                     "sku_id": sku,
#                     "product_id": f"prod_{int(sku.split('_')[1])//10}",
#                     "shop_id": random.choice(shop_ids),
#                     "pay_count": pay_count,
#                     "pay_amount": round(pay_count * price, 2),
#                     "sales_ratio": sales_ratio,
#                     "stock_count": stock,
#                     "stock_days": stock_days,
#                     "data_dt": dt,
#                     "time_type": tt,
#                     "dt": dt
#                 })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_sku_sales_detail.csv", index=False, sep="\t")
#     print("生成ods_sku_sales_detail完成")
#
# # 4. 商品搜索词表（ods_product_search_words）
# def generate_product_search_words():
#     data = []
#     product_ids = [f"prod_{i}" for i in range(1000, 1500)]
#     search_words = ["轩妈家", "新款", "优惠", "正品", "热销", "性价比", "网红款", "限量"]  # 包含文档中的示例词
#     for dt in date_list:
#         for tt in time_types:
#             for prod_id in random.sample(product_ids, 40):
#                 for word in random.sample(search_words, 3):  # 每个商品3个搜索词
#                     data.append({
#                         "product_id": prod_id,
#                         "shop_id": random.choice(shop_ids),
#                         "search_word": word,
#                         "visitor_count": random.randint(50, 3000),
#                         "data_dt": dt,
#                         "time_type": tt,
#                         "dt": dt
#                     })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_product_search_words.csv", index=False, sep="\t")
#     print("生成ods_product_search_words完成")
#
# # 5. 价格力商品信息表（ods_price_strength_product）
# def generate_price_strength_product():
#     data = []
#     product_ids = [f"prod_{i}" for i in range(1000, 1500)]
#     for dt in date_list:
#         for prod_id in random.sample(product_ids, 60):  # 每天60个价格力商品
#             strength_level = random.choice(["优秀", "良好", "较差"])
#             is_warn = "是" if strength_level == "较差" else "否"
#             warn_type = "低价格力" if is_warn == "是" else ""
#             data.append({
#                 "product_id": prod_id,
#                 "shop_id": random.choice(shop_ids),
#                 "price_strength_level": strength_level,
#                 "product_strength_score": random.randint(50, 100),
#                 "coupon_after_price": round(random.uniform(10, 800), 2),
#                 "is_warn": is_warn,
#                 "warn_type": warn_type,
#                 "data_dt": dt,
#                 "dt": dt
#             })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_price_strength_product.csv", index=False, sep="\t")
#     print("生成ods_price_strength_product完成")
#
# # 6. 商品访客与支付表（ods_product_visitor_pay）
# def generate_product_visitor_pay():
#     data = []
#     product_ids = [f"prod_{i}" for i in range(1000, 1500)]
#     for dt in date_list:
#         for tt in time_types:
#             for prod_id in random.sample(product_ids, 50):
#                 visitors = random.randint(200, 10000)
#                 pay_buyers = random.randint(5, int(visitors * 0.3))  # 支付买家<=30%访客
#                 pay_conv = round(pay_buyers / visitors, 4) if visitors > 0 else 0
#                 data.append({
#                     "product_id": prod_id,
#                     "shop_id": random.choice(shop_ids),
#                     "visitor_count": visitors,
#                     "pay_buyer_count": pay_buyers,
#                     "pay_conversion_rate": pay_conv,
#                     "data_dt": dt,
#                     "time_type": tt,
#                     "dt": dt
#                 })
#     df = pd.DataFrame(data)
#     df.to_csv(f"{output_dir}/ods_product_visitor_pay.csv", index=False, sep="\t")
#     print("生成ods_product_visitor_pay完成")
#
# # 执行所有生成函数
# if __name__ == "__main__":
#     generate_sku_base_info()
#     generate_product_traffic_source()
#     generate_sku_sales_detail()
#     generate_product_search_words()
#     generate_price_strength_product()
#     generate_product_visitor_pay()
#     print("所有ODS层模拟数据生成完成，存储路径：", os.path.abspath(output_dir))