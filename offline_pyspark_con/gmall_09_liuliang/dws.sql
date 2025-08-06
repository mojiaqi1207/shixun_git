use worker09;
CREATE TABLE IF NOT EXISTS dws_ecommerce_traffic_summary (
                                                             `date` STRING COMMENT '日期',
                                                             time_granularity STRING COMMENT '时间粒度（day/7day/30day/month）',
                                                             device_type STRING COMMENT '设备类型',
                                                             entry_page_type STRING COMMENT '进店页面类型',
                                                             entry_page STRING COMMENT '进店页面',
                                                             total_visitor_count INT COMMENT '总访客数',
                                                             total_order_count INT COMMENT '总下单买家数',
                                                             avg_stay_duration DOUBLE COMMENT '平均停留时长（秒）',
                                                             order_conversion_rate DOUBLE COMMENT '下单转化率（下单数/访客数）'
)
    COMMENT '按维度聚合的电商流量汇总数据，支持多粒度查询'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/dws/dws_ecommerce_traffic_summary';


-- 日粒度数据
INSERT OVERWRITE TABLE dws_ecommerce_traffic_summary
SELECT
    `date`,
    'day' AS time_granularity,
    device_type,
    entry_page_type,
    entry_page,
    SUM(visitor_count) AS total_visitor_count,
    SUM(order_count) AS total_order_count,
    AVG(avg_stay_duration) AS avg_stay_duration,
    -- 计算转化率（保留4位小数）
    ROUND(SUM(order_count) / SUM(visitor_count), 4) AS order_conversion_rate
FROM dwd_ecommerce_traffic_detail
GROUP BY `date`, device_type, entry_page_type, entry_page;

-- 7天粒度数据（可通过调度定期生成）
INSERT INTO TABLE dws_ecommerce_traffic_summary
SELECT
    MAX(`date`) AS `date`, -- 取结束日期
    '7day' AS time_granularity,
    device_type,
    entry_page_type,
    entry_page,
    SUM(visitor_count) AS total_visitor_count,
    SUM(order_count) AS total_order_count,
    AVG(avg_stay_duration) AS avg_stay_duration,
    ROUND(SUM(order_count) / SUM(visitor_count), 4) AS order_conversion_rate
FROM dwd_ecommerce_traffic_detail
WHERE `date` BETWEEN DATE_SUB(CURRENT_DATE(), 6) AND CURRENT_DATE()
GROUP BY device_type, entry_page_type, entry_page;


select * from dws_ecommerce_traffic_summary;