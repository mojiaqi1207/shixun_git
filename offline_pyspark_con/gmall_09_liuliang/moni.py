import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid

# 生成数据量（超过100万条）
DATA_SIZE = 1000000

# 定义页面类型和具体页面列表
page_types = {
    "店铺页": ["首页", "活动页", "分类页", "宝贝页", "新品页"],
    "商品详情页": [f"商品详情页_{i}" for i in range(1, 201)],  # 200个商品详情页
    "店铺其他页": ["订阅页", "直播页", "客服页", "订单页"]
}

# 所有可能的页面
all_pages = []
for page_type, pages in page_types.items():
    all_pages.extend(pages)

# 生成随机日期（近30天）
start_date = datetime.now() - timedelta(days=30)
dates = [start_date + timedelta(days=random.randint(0, 29)) for _ in range(DATA_SIZE)]

# 生成访客ID
visitor_ids = [str(uuid.uuid4()) for _ in range(DATA_SIZE)]

# 生成设备类型（无线端/PC端）
device_types = np.random.choice(["无线端", "PC端"], size=DATA_SIZE, p=[0.8, 0.2])

# 生成进店页面
entry_pages = np.random.choice(all_pages, size=DATA_SIZE)

# 生成进店页面类型
entry_page_types = []
for page in entry_pages:
    for page_type, pages in page_types.items():
        if page in pages:
            entry_page_types.append(page_type)
            break

# 生成访客数（模拟，1-5之间随机，代表该页面该时段的访客量）
visitor_counts = np.random.randint(1, 6, size=DATA_SIZE)

# 生成下单买家数（部分访客会下单）
order_counts = []
for count in visitor_counts:
    order_prob = random.uniform(0.05, 0.3)  # 5%-30%的下单概率
    order_counts.append(max(0, int(count * order_prob)))

# 生成平均停留时长（秒）
stay_durations = np.random.randint(5, 300, size=DATA_SIZE)  # 5秒到5分钟

# 生成来源页面（可能为空，即直接进店）
source_pages = []
for _ in range(DATA_SIZE):
    if random.random() < 0.2:  # 20%概率直接进店，无来源
        source_pages.append("直接进店")
    else:
        source_pages.append(random.choice(all_pages))

# 生成去向页面（可能多个，用逗号分隔）
destination_pages = []
for _ in range(DATA_SIZE):
    num_destinations = random.randint(1, 3)  # 1-3个去向页面
    dests = random.sample(all_pages, num_destinations)
    destination_pages.append(",".join(dests))

# 构建数据框
ods_data = pd.DataFrame({
    "date": [d.strftime("%Y-%m-%d") for d in dates],
    "visitor_id": visitor_ids,
    "device_type": device_types,
    "entry_page": entry_pages,
    "entry_page_type": entry_page_types,
    "visitor_count": visitor_counts,
    "order_count": order_counts,
    "avg_stay_duration": stay_durations,
    "source_page": source_pages,
    "destination_pages": destination_pages,
    "create_time": [datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in range(DATA_SIZE)],
    "data_source": "生意参谋"
})

# 保存为CSV文件（可根据需要改为其他格式如Parquet）
output_path = "ods_ecommerce_traffic_data.csv"
ods_data.to_csv(output_path, index=False, encoding="utf-8")

print(f"ODS层数据生成完成，共{DATA_SIZE}条记录，已保存至：{output_path}")
print(f"数据示例：")
print(ods_data.head())