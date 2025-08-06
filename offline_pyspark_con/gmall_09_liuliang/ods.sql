create database worker09;
use worker09;
CREATE TABLE IF NOT EXISTS ods_ecommerce_traffic (
                                                     `date` STRING COMMENT '日期',
                                                     visitor_id STRING COMMENT '访客ID',
                                                     device_type STRING COMMENT '设备类型（无线端/PC端）',
                                                     entry_page STRING COMMENT '进店页面',
                                                     entry_page_type STRING COMMENT '进店页面类型（店铺页/商品详情页/店铺其他页）',
                                                     visitor_count INT COMMENT '访客数',
                                                     order_count INT COMMENT '下单买家数',
                                                     avg_stay_duration INT COMMENT '平均停留时长（秒）',
                                                     source_page STRING COMMENT '来源页面',
                                                     destination_pages STRING COMMENT '去向页面（多值用逗号分隔）',
                                                     create_time STRING COMMENT '数据创建时间',
                                                     data_source STRING COMMENT '数据源（生意参谋）',
                                                     work_order_id STRING COMMENT '工单编号：大数据-电商数仓-09-流量主题店内路径看板'
)
    COMMENT '电商流量原始数据，数据量大于100万条'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/ods/ods_ecommerce_traffic';


LOAD DATA  INPATH '/warehouse/worker09/ods_ecommerce_traffic_data.csv'
    OVERWRITE INTO TABLE ods_ecommerce_traffic;


select * from ods_ecommerce_traffic;