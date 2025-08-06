-- 建表
use worker09;

-- 无线端入店与承接看板表
CREATE TABLE IF NOT EXISTS ads_wireless_entry_board (
                                                        `date` STRING COMMENT '日期',
                                                        time_granularity STRING COMMENT '时间粒度',
                                                        entry_page STRING COMMENT '进店页面',
                                                        entry_page_type STRING COMMENT '页面类型',
                                                        visitor_count INT COMMENT '访客数',
                                                        order_count INT COMMENT '下单买家数',
                                                        order_conversion_rate DOUBLE COMMENT '下单转化率'
)
    COMMENT '无线端入店数据看板，展示访客数、下单数等指标'
    STORED AS PARQUET;

-- 数据同步
INSERT OVERWRITE TABLE ads_wireless_entry_board
SELECT
    `date`,
    time_granularity,
    entry_page,
    entry_page_type,
    total_visitor_count AS visitor_count,
    total_order_count AS order_count,
    order_conversion_rate
FROM dws_ecommerce_traffic_summary
WHERE device_type = '无线端';


-- 建表页面访问排行表
CREATE TABLE IF NOT EXISTS ads_page_visit_rank (
                                                   `rank` INT COMMENT '排名',
                                                   entry_page STRING COMMENT '页面名称',
                                                   entry_page_type STRING COMMENT '页面类型',
                                                   total_visitor_count INT COMMENT '总访客数',
                                                   time_granularity STRING COMMENT '时间粒度'
)
    COMMENT '页面访问排行，按访客数排序'
    STORED AS PARQUET;

-- 数据同步（取TOP20）
INSERT OVERWRITE TABLE ads_page_visit_rank
SELECT
    ROW_NUMBER() OVER (ORDER BY total_visitor_count DESC) AS `rank`,
        entry_page,
    entry_page_type,
    total_visitor_count,
    time_granularity
FROM dws_ecommerce_traffic_summary
WHERE time_granularity = 'day'
    LIMIT 20;




-- 建表店内路径流转表
CREATE TABLE IF NOT EXISTS ads_instore_flow_path (
                                                     source_page STRING COMMENT '来源页面',
                                                     destination_page STRING COMMENT '去向页面',
                                                     flow_count INT COMMENT '流转次数',
                                                     flow_rate DOUBLE COMMENT '流转占比（该路径流量/总流量）'
)
    COMMENT '店内路径流转数据，展示页面间的访客流转情况'
    STORED AS PARQUET;

-- 数据同步
INSERT OVERWRITE TABLE ads_instore_flow_path
SELECT
    source_page,
    destination_page,
    SUM(visitor_count) AS flow_count,
    -- 计算流转占比
    ROUND(SUM(visitor_count) / total.total_visitors, 4) AS flow_rate
FROM dwd_ecommerce_traffic_detail
         CROSS JOIN (SELECT SUM(visitor_count) AS total_visitors FROM dwd_ecommerce_traffic_detail) total
GROUP BY source_page, destination_page, total.total_visitors
ORDER BY flow_count DESC;
