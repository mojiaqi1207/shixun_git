
create database work01;
use work01;
-- 1. 商品访问日志（增量）
CREATE EXTERNAL TABLE IF NOT EXISTS ods_trd_item_visit_inc (
                                                                   visit_id          STRING,      -- 访问唯一ID
                                                                   user_id           STRING,      -- 用户ID
                                                                   item_id           STRING,      -- 商品ID
                                                                   shop_id           STRING,      -- 店铺ID
                                                                   visit_time        TIMESTAMP,   -- 访问时间
                                                                   channel           STRING,      -- 终端 pc/wireless
                                                                   stay_seconds      INT,         -- 停留时长
                                                                   bounce_flag       TINYINT      -- 1:跳出
) PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS ORC
    LOCATION '/dw/ods/trd_item_visit/';

-- 2. 商品收藏日志
CREATE EXTERNAL TABLE IF NOT EXISTS ods_trd_item_fav_inc (
                                                                 fav_id            STRING,
                                                                 user_id           STRING,
                                                                 item_id           STRING,
                                                                 fav_time          TIMESTAMP
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ods/trd_item_fav/';

-- 3. 商品加购日志
CREATE EXTERNAL TABLE IF NOT EXISTS ods_trd_item_cart_inc (
                                                                  cart_id           STRING,
                                                                  user_id           STRING,
                                                                  item_id           STRING,
                                                                  cart_cnt          INT,
                                                                  cart_time         TIMESTAMP
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ods/trd_item_cart/';

-- 4. 订单明细（增量）
CREATE EXTERNAL TABLE IF NOT EXISTS ods_trd_order_detail_inc (
                                                                     order_id          STRING,
                                                                     user_id           STRING,
                                                                     item_id           STRING,
                                                                     shop_id           STRING,
                                                                     pay_amount        DECIMAL(18,2),
                                                                     pay_cnt           INT,
                                                                     pay_time          TIMESTAMP,
                                                                     is_new_buyer      TINYINT      -- 1:新买家
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ods/trd_order_detail/';

-- 5. 商品维表（全量）
CREATE EXTERNAL TABLE IF NOT EXISTS ods_item_info_df (
                                                             item_id           STRING,
                                                             item_name         STRING,
                                                             category1_id      STRING,
                                                             category2_id      STRING,
                                                             category3_id      STRING,
                                                             price             DECIMAL(18,2),
                                                             shop_id           STRING
) PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/ods/item_info/';



-- 1.1 商品访问日志
INSERT OVERWRITE TABLE ods_trd_item_visit_inc PARTITION(dt='2025-08-09')
SELECT
    reflect('java.util.UUID','randomUUID') AS visit_id,
    concat('u',lpad(cast(rand()*10000 as int),5,'0')) AS user_id,
    concat('i',lpad(cast(rand()*400+1 as int),4,'0')) AS item_id,
    concat('s',lpad(cast(rand()*20+1 as int),2,'0')) AS shop_id,
    from_unixtime(unix_timestamp('2025-08-09') + cast(rand()*86400 as int)) AS visit_time,
    if(rand()>0.5,'pc','wireless') AS channel,
    cast(rand()*120 as int) AS stay_seconds,
    cast(if(rand()<0.35,1,0) as tinyint) AS bounce_flag
FROM (SELECT 1) t
    LATERAL VIEW explode(split(space(2000),'')) e as x;

-- 1.2 商品收藏日志
INSERT OVERWRITE TABLE ods_trd_item_fav_inc PARTITION(dt='2025-08-09')
SELECT
    reflect('java.util.UUID','randomUUID'),
    concat('u',lpad(cast(rand()*10000 as int),5,'0')),
    concat('i',lpad(cast(rand()*400+1 as int),4,'0')),
    from_unixtime(unix_timestamp('2025-08-09')+cast(rand()*86400 as int))
FROM (SELECT 1) t
    LATERAL VIEW explode(split(space(2000),'')) e as x;

-- 1.3 商品加购日志
INSERT OVERWRITE TABLE ods_trd_item_cart_inc PARTITION(dt='2025-08-09')
SELECT
    reflect('java.util.UUID','randomUUID'),
    concat('u',lpad(cast(rand()*10000 as int),5,'0')),
    concat('i',lpad(cast(rand()*400+1 as int),4,'0')),
    cast(rand()*3+1 as int),
    from_unixtime(unix_timestamp('2025-08-09')+cast(rand()*86400 as int))
FROM (SELECT 1) t
    LATERAL VIEW explode(split(space(2000),'')) e as x;

-- 1.4 订单明细
INSERT OVERWRITE TABLE ods_trd_order_detail_inc PARTITION(dt='2025-08-09')
SELECT
    concat('o',lpad(cast(rand()*5000 as int),6,'0')),
    concat('u',lpad(cast(rand()*10000 as int),5,'0')),
    concat('i',lpad(cast(rand()*400+1 as int),4,'0')),
    concat('s',lpad(cast(rand()*20+1 as int),2,'0')),
    cast(rand()*2000+50 as DECIMAL(18,2)),
    cast(rand()*2+1 as int),
    from_unixtime(unix_timestamp('2025-08-09')+cast(rand()*86400 as int)),
    cast(if(rand()<0.3,1,0) as tinyint) AS is_new_buyer
FROM (SELECT 1) t
    LATERAL VIEW explode(split(space(2000),'')) e as x;

-- 1.5 商品维表（全量 400 条）
INSERT OVERWRITE TABLE ods_item_info_df PARTITION(dt='2025-08-09')
SELECT
    concat('i',lpad(cast(row_number() over() as string),4,'0')) AS item_id,
    concat('商品',cast(row_number() over() as string))          AS item_name,
    concat('c1_',cast(rand()*5+1 as int))                       AS category1_id,
    concat('c2_',cast(rand()*20+1 as int))                      AS category2_id,
    concat('c3_',cast(rand()*100+1 as int))                     AS category3_id,
    cast(rand()*1000+20 as DECIMAL(18,2))                       AS price,
    concat('s',lpad(cast(rand()*20+1 as int),2,'0'))            AS shop_id
FROM (SELECT 1) t
    LATERAL VIEW explode(split(space(400),'')) e as x;