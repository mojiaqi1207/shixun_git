use work01;
-- 1. 商品宏观监控看板（日）
CREATE EXTERNAL TABLE IF NOT EXISTS ads_item_macro_dashboard_di (
                                                                        stat_date         STRING,
                                                                        shop_id           STRING,
                                                                        category1_id      STRING,
                                                                        category2_id      STRING,
                                                                        category3_id      STRING,
                                                                        price_band        STRING,      -- 价格带
                                                                        qty_band          STRING,      -- 支付件数带
                                                                        amt_band          STRING,      -- 支付金额带
                                                                        item_cnt          BIGINT,      -- 动销商品数
                                                                        uv                BIGINT,
                                                                        pv                BIGINT,
                                                                        pay_cnt           BIGINT,
                                                                        pay_amount        DECIMAL(18,2),
                                                                        pay_rate          DECIMAL(10,4),
                                                                        per_uv_value      DECIMAL(18,2),
                                                                        avg_price         DECIMAL(18,2),
                                                                        bounce_rate       DECIMAL(10,4),
                                                                        fav_rate          DECIMAL(10,4),
                                                                        cart_rate         DECIMAL(10,4)
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ads/item_macro_dashboard_di/';

-- 2. 店铺宏观监控看板（日）
CREATE EXTERNAL TABLE IF NOT EXISTS ads_shop_macro_dashboard_di (
                                                                        stat_date         STRING,
                                                                        shop_id           STRING,
                                                                        uv                BIGINT,
                                                                        pay_amount        DECIMAL(18,2),
                                                                        pay_cnt           BIGINT,
                                                                        pay_uv            BIGINT,
                                                                        new_pay_uv        BIGINT,
                                                                        old_pay_uv        BIGINT,
                                                                        per_uv_value      DECIMAL(18,2),
                                                                        pay_rate          DECIMAL(10,4)
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ads/shop_macro_dashboard_di/';



-- 4.1 商品宏观监控看板
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;
WITH banded AS (
    SELECT
        t.shop_id,
        i.category1_id,
        i.category2_id,
        i.category3_id,
        CASE
            WHEN i.price BETWEEN 0  AND 50  THEN '0-50'
            WHEN i.price BETWEEN 51 AND 100 THEN '51-100'
            WHEN i.price BETWEEN 101 AND 200 THEN '101-200'
            ELSE '200+'
            END AS price_band,
        CASE
            WHEN t.pay_cnt BETWEEN 0  AND 50   THEN '0-50'
            WHEN t.pay_cnt BETWEEN 51 AND 100  THEN '51-100'
            WHEN t.pay_cnt BETWEEN 101 AND 150 THEN '101-150'
            ELSE '150+'
            END AS qty_band,
        CASE
            WHEN t.pay_amount BETWEEN 0    AND 1000  THEN '0-1000'
            WHEN t.pay_amount BETWEEN 1001 AND 5000  THEN '1001-5000'
            WHEN t.pay_amount BETWEEN 5001 AND 10000 THEN '5001-10000'
            ELSE '10000+'
            END AS amt_band,
        uv,
        pv,
        pay_cnt,
        pay_amount,
        pay_uv,
        bounce_rate,
        fav_rate,
        cart_rate
    FROM dws_item_trd_1d t
             JOIN dim_item_info i
                  ON t.item_id = i.item_id
                      AND i.is_current = 1
    WHERE t.dt = '2025-08-09'
)
INSERT OVERWRITE TABLE ads_item_macro_dashboard_di PARTITION(dt='2025-08-09')
SELECT
    '2025-08-09' AS stat_date,
    shop_id,
    category1_id,
    category2_id,
    category3_id,
    price_band,
    qty_band,
    amt_band,
    COUNT(*) AS item_cnt,
    SUM(uv)  AS uv,
    SUM(pv)  AS pv,
    SUM(pay_cnt)       AS pay_cnt,
    SUM(pay_amount)    AS pay_amount,
    CAST(SUM(pay_uv) /
         CASE WHEN SUM(uv)=0 THEN NULL ELSE SUM(uv) END
        AS DECIMAL(10,4)) AS pay_rate,
    CAST(SUM(pay_amount) /
         CASE WHEN SUM(uv)=0 THEN NULL ELSE SUM(uv) END
        AS DECIMAL(18,2)) AS per_uv_value,
    CAST(SUM(pay_amount) /
         CASE WHEN SUM(pay_cnt)=0 THEN NULL ELSE SUM(pay_cnt) END
        AS DECIMAL(18,2)) AS avg_price,
    CAST(SUM(uv * bounce_rate) /
         CASE WHEN SUM(uv)=0 THEN NULL ELSE SUM(uv) END
        AS DECIMAL(10,4)) AS bounce_rate,
    CAST(SUM(uv * fav_rate) /
         CASE WHEN SUM(uv)=0 THEN NULL ELSE SUM(uv) END
        AS DECIMAL(10,4)) AS fav_rate,
    CAST(SUM(uv * cart_rate) /
         CASE WHEN SUM(uv)=0 THEN NULL ELSE SUM(uv) END
        AS DECIMAL(10,4)) AS cart_rate
FROM banded
GROUP BY
    shop_id,
    category1_id,
    category2_id,
    category3_id,
    price_band,
    qty_band,
    amt_band;


-- 4.2 店铺宏观监控看板
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;
INSERT OVERWRITE TABLE ads_shop_macro_dashboard_di PARTITION(dt='2025-08-09')
SELECT
    '2025-08-09' AS stat_date,
    shop_id,
    uv,
    pay_amount,
    pay_cnt,
    pay_uv,
    new_pay_uv,
    old_pay_uv,

    -- 人均价值
    CAST(pay_amount /
         CASE WHEN uv = 0 THEN NULL ELSE uv END
        AS DECIMAL(18,2)) AS per_uv_value,

    -- 支付转化率
    CAST(pay_uv /
         CASE WHEN uv = 0 THEN NULL ELSE uv END
        AS DECIMAL(10,4)) AS pay_rate

FROM dws_shop_trd_1d
WHERE dt = '2025-08-09';