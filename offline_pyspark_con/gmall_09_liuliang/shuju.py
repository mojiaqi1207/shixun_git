# import random
# import time
# import string
#
# # 生成随机字符串（页面ID/用户ID）
# def random_str(length=8):
#     return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
#
# # 页面类型与细分类型映射
# page_type_map = {
#     'shop': ['home', 'activity', 'category', 'new'],  # 店铺页：首页/活动页/分类页/新品页
#     'product': ['detail'],  # 商品详情页
#     'other': ['subscribe', 'live']  # 其他页：订阅页/直播页
# }
#
# # 生成100万条数据
# with open('ods_traffic_instore_path.txt', 'w', encoding='utf-8') as f:
#     for i in range(1000000):
#         user_id = random_str(10)  # 用户ID
#         # 访问时间（近30天内随机）
#         timestamp = random.randint(int(time.time()) - 30*86400, int(time.time()))
#         visit_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
#         # 页面类型与ID
#         page_type = random.choice(list(page_type_map.keys()))
#         page_subtype = random.choice(page_type_map[page_type])
#         page_id = f"{page_type}_{page_subtype}_{random_str(6)}"
#         # 来源页面（可能为空，模拟首次访问）
#         source_page_type = random.choice(list(page_type_map.keys()) + [None]) if random.random() > 0.3 else None
#         if source_page_type:
#             source_subtype = random.choice(page_type_map[source_page_type])
#             source_page_id = f"{source_page_type}_{source_subtype}_{random_str(6)}"
#         else:
#             source_page_id = None
#         # 终端类型
#         terminal_type = 'wireless' if random.random() > 0.2 else 'pc'  # 80%无线端，20%PC端
#         # 停留时长（5-300秒）
#         stay_duration = random.randint(5, 300)
#         # 是否下单（1%概率）
#         is_order = 1 if random.random() < 0.01 else 0
#
#         # 拼接数据行（空值用'\N'表示）
#         line = '\t'.join([
#             user_id,
#             visit_time,
#             page_type,
#             page_id,
#             source_page_type or '\\N',
#             source_page_id or '\\N',
#             terminal_type,
#             str(stay_duration),
#             str(is_order)
#         ])
#         f.write(line + '\n')
#
#         # 进度提示
#         if i % 100000 == 0:
#             print(f"已生成 {i} 条数据")
#
# print("100万条数据生成完成！")