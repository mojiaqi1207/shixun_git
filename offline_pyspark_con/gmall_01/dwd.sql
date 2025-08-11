use work01;
-- 1. 商品访问明细（天粒度）
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_item_visit_di (
                                                              item_id           STRING,
                                                              user_id           STRING,
                                                              shop_id           STRING,
                                                              visit_date        STRING,
                                                              visit_cnt         INT,
                                                              stay_seconds      INT,
                                                              bounce_flag       TINYINT,
                                                              channel           STRING
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dwd/item_visit_di/';

-- 2. 商品收藏明细
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_item_fav_di (
                                                            item_id           STRING,
                                                            user_id           STRING,
                                                            shop_id           STRING,
                                                            fav_date          STRING,
                                                            fav_cnt           INT
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dwd/item_fav_di/';

-- 3. 商品加购明细
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_item_cart_di (
                                                             item_id           STRING,
                                                             user_id           STRING,
                                                             shop_id           STRING,
                                                             cart_date         STRING,
                                                             cart_cnt          INT
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dwd/item_cart_di/';

-- 4. 订单明细
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_item_order_di (
                                                              item_id           STRING,
                                                              user_id           STRING,
                                                              shop_id           STRING,
                                                              order_date        STRING,
                                                              pay_cnt           INT,
                                                              pay_amount        DECIMAL(18,2),
                                                              is_new_buyer      TINYINT
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dwd/item_order_di/';

-- 5. 商品维表（拉链）
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_item_info (
                                                          item_id           STRING,
                                                          item_name         STRING,
                                                          category1_id      STRING,
                                                          category2_id      STRING,
                                                          category3_id      STRING,
                                                          price             DECIMAL(18,2),
                                                          shop_id           STRING,
                                                          start_dt          STRING,
                                                          end_dt            STRING,
                                                          is_current        TINYINT
) STORED AS ORC
    LOCATION '/dw/dim/item_info/';


CREATE EXTERNAL TABLE IF NOT EXISTS dim_item_info (
                                                          item_id           STRING,
                                                          item_name         STRING,
                                                          category1_id      STRING,
                                                          category2_id      STRING,
                                                          category3_id      STRING,
                                                          price             DECIMAL(18,2),
                                                          shop_id           STRING,
                                                          start_dt          STRING,
                                                          end_dt            STRING,
                                                          is_current        TINYINT
) STORED AS ORC
    LOCATION '/dw/dim/item_info/';

-- 2.1 商品访问明细
INSERT OVERWRITE TABLE dwd_item_visit_di PARTITION(dt='2025-08-09')
SELECT
    item_id,
    shop_id,
    '2025-08-09' AS visit_date,
    1            AS visit_cnt,
    stay_seconds,
    bounce_flag,
    channel,
    user_id
FROM ods_trd_item_visit_inc
WHERE dt='2025-08-09';

-- 2.2 商品收藏明细
-- 2.2 商品收藏明细（补 shop_id）
-- 仅当前 session 生效
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;           -- 本地 MR 进程最大堆内存
INSERT OVERWRITE TABLE dwd_item_fav_di PARTITION(dt='2025-08-09')
SELECT
    f.item_id,
    i.shop_id,                    -- 来自维表
    '2025-08-09' AS fav_date,
    1            AS fav_cnt,
    f.user_id
FROM ods_trd_item_fav_inc f
         JOIN ods_item_info_df i
              ON f.item_id = i.item_id
WHERE f.dt='2025-08-09'
  AND i.dt='2025-08-09';        -- 维表也按 dt 过滤

-- 2.3 商品加购明细（补 shop_id）
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;           -- 本地 MR 进程最大堆内存
INSERT OVERWRITE TABLE dwd_item_cart_di PARTITION(dt='2025-08-09')
SELECT
    c.item_id,
    i.shop_id,                    -- 来自维表
    '2025-08-09' AS cart_date,
    c.cart_cnt,
    c.user_id
FROM ods_trd_item_cart_inc c
         JOIN ods_item_info_df i
              ON c.item_id = i.item_id
WHERE c.dt='2025-08-09'
  AND i.dt='2025-08-09';

-- 2.4 订单明细
INSERT OVERWRITE TABLE dwd_item_order_di PARTITION(dt='2025-08-09')
SELECT
    item_id,
    shop_id,
    '2025-08-09' AS order_date,
    pay_cnt,
    pay_amount,
    is_new_buyer,
    user_id
FROM ods_trd_order_detail_inc
WHERE dt='2025-08-09';


set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;
INSERT OVERWRITE TABLE dim_item_info
SELECT
    item_id,
    item_name,
    category1_id,
    category2_id,
    category3_id,
    price,
    shop_id,
    '2025-08-09' AS start_dt,
    '9999-12-31' AS end_dt,
    1            AS is_current
FROM ods_item_info_df
WHERE dt='2025-08-09';