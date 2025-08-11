use work01;

-- 1. 商品日汇总
CREATE EXTERNAL TABLE IF NOT EXISTS dws_item_trd_1d (
                                                            item_id           STRING,
                                                            shop_id           STRING,
                                                            stat_date         STRING,
    -- 访问
                                                            uv                BIGINT,
                                                            pv                BIGINT,
                                                            stay_seconds      BIGINT,
                                                            bounce_rate       DECIMAL(10,4),
    -- 收藏
                                                            fav_cnt           BIGINT,
                                                            fav_rate          DECIMAL(10,4),
    -- 加购
                                                            cart_cnt          BIGINT,
                                                            cart_rate         DECIMAL(10,4),
    -- 支付
                                                            pay_cnt           BIGINT,
                                                            pay_amount        DECIMAL(18,2),
                                                            pay_uv            BIGINT,
                                                            pay_rate          DECIMAL(10,4),
                                                            new_pay_uv        BIGINT,
                                                            old_pay_uv        BIGINT,
    -- 价值
                                                            per_uv_value      DECIMAL(18,2),
                                                            avg_price         DECIMAL(18,2)
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dws/item_trd_1d/';

-- 2. 店铺日汇总（示例）
CREATE EXTERNAL TABLE IF NOT EXISTS dws_shop_trd_1d (
                                                            shop_id           STRING,
                                                            stat_date         STRING,
                                                            uv                BIGINT,
                                                            pay_amount        DECIMAL(18,2),
                                                            pay_cnt           BIGINT,
                                                            pay_uv            BIGINT,
                                                            new_pay_uv        BIGINT,
                                                            old_pay_uv        BIGINT
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/dws/shop_trd_1d/';


-- 3.1 商品日汇总
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;
INSERT OVERWRITE TABLE dws_item_trd_1d PARTITION(dt='2025-08-09')
SELECT
    v.item_id,
    v.shop_id,
    '2025-08-09' AS stat_date,

    -- 访问
    COUNT(DISTINCT v.user_id)                                                     AS uv,
    SUM(v.visit_cnt)                                                              AS pv,
    SUM(v.stay_seconds)                                                           AS stay_seconds,
    CAST(SUM(v.bounce_flag) / COUNT(*) AS DECIMAL(10,4))                          AS bounce_rate,

    -- 收藏
    COALESCE(f.fav_cnt,0)                                                         AS fav_cnt,
    CAST(COALESCE(f.fav_cnt,0) /
         CASE WHEN COUNT(DISTINCT v.user_id)=0 THEN NULL
              ELSE COUNT(DISTINCT v.user_id)
             END AS DECIMAL(10,4))                                                    AS fav_rate,

    -- 加购
    COALESCE(c.cart_cnt,0)                                                        AS cart_cnt,
    CAST(COALESCE(c.cart_cnt,0) /
         CASE WHEN COUNT(DISTINCT v.user_id)=0 THEN NULL
              ELSE COUNT(DISTINCT v.user_id)
             END AS DECIMAL(10,4))                                                    AS cart_rate,

    -- 支付
    COALESCE(o.pay_cnt,0)                                                         AS pay_cnt,
    COALESCE(o.pay_amount,0)                                                      AS pay_amount,
    COALESCE(o.pay_uv,0)                                                          AS pay_uv,
    CAST(COALESCE(o.pay_uv,0) /
         CASE WHEN COUNT(DISTINCT v.user_id)=0 THEN NULL
              ELSE COUNT(DISTINCT v.user_id)
             END AS DECIMAL(10,4))                                                    AS pay_rate,
    COALESCE(o.new_pay_uv,0)                                                      AS new_pay_uv,
    COALESCE(o.old_pay_uv,0)                                                      AS old_pay_uv,

    -- 价值指标
    CAST(COALESCE(o.pay_amount,0) /
         CASE WHEN COUNT(DISTINCT v.user_id)=0 THEN NULL
              ELSE COUNT(DISTINCT v.user_id)
             END AS DECIMAL(18,2))                                                    AS per_uv_value,
    CAST(COALESCE(o.pay_amount,0) /
         CASE WHEN COALESCE(o.pay_cnt,0)=0 THEN NULL
              ELSE COALESCE(o.pay_cnt,0)
             END AS DECIMAL(18,2))                                                    AS avg_price

FROM dwd_item_visit_di v
         LEFT JOIN (
    SELECT item_id,
           shop_id,
           COUNT(*) AS fav_cnt
    FROM   dwd_item_fav_di
    WHERE  dt = '2025-08-09'
    GROUP  BY item_id, shop_id
) f
                   ON v.item_id = f.item_id AND v.shop_id = f.shop_id

         LEFT JOIN (
    SELECT item_id,
           shop_id,
           SUM(cart_cnt) AS cart_cnt
    FROM   dwd_item_cart_di
    WHERE  dt = '2025-08-09'
    GROUP  BY item_id, shop_id
) c
                   ON v.item_id = c.item_id AND v.shop_id = c.shop_id

         LEFT JOIN (
    SELECT item_id,
           shop_id,
           SUM(pay_cnt)                                      AS pay_cnt,
           SUM(pay_amount)                                   AS pay_amount,
           COUNT(DISTINCT user_id)                           AS pay_uv,
           COUNT(DISTINCT CASE WHEN is_new_buyer = 1 THEN user_id END) AS new_pay_uv,
           COUNT(DISTINCT CASE WHEN is_new_buyer = 0 THEN user_id END) AS old_pay_uv
    FROM   dwd_item_order_di
    WHERE  dt = '2025-08-09'
    GROUP  BY item_id, shop_id
) o
                   ON v.item_id = o.item_id AND v.shop_id = o.shop_id

WHERE v.dt = '2025-08-09'

GROUP BY
    v.item_id,
    v.shop_id,
    COALESCE(f.fav_cnt,0),
    COALESCE(c.cart_cnt,0),
    COALESCE(o.pay_cnt,0),
    COALESCE(o.pay_amount,0),
    COALESCE(o.pay_uv,0),
    COALESCE(o.new_pay_uv,0),
    COALESCE(o.old_pay_uv,0);

-- 3.2 店铺日汇总
set hive.auto.convert.join=false;
set hive.mapred.local.mem=4096;
INSERT OVERWRITE TABLE dws_shop_trd_1d PARTITION(dt='2025-08-09')
SELECT
    shop_id,
    '2025-08-09',
    SUM(uv)               AS uv,
    SUM(pay_amount)       AS pay_amount,
    SUM(pay_cnt)          AS pay_cnt,
    SUM(pay_uv)           AS pay_uv,
    SUM(new_pay_uv)       AS new_pay_uv,
    SUM(old_pay_uv)       AS old_pay_uv
FROM dws_item_trd_1d
WHERE dt='2025-08-09'
GROUP BY shop_id;