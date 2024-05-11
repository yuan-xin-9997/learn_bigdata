-- ODS层全量业务表建立

DROP TABLE IF EXISTS ods_activity_info_full;
CREATE EXTERNAL TABLE ods_activity_info_full(
    id bigint,
    activity_name string,
    activity_type string,
    activity_desc string,
    start_time string,
    end_time string,
    create_time string
)
    partitioned by (dt string)
ROW FORMAT DELIMITED fields terminated by '\t' null defined as ''
STORED AS TEXTFILE
location '/warehouse/gmall/ods/ods_activity_info_full/';

-- 导入数据
load data inpath '/origin_data/gmall/db/activity_info_full/2020-06-14' overwrite into table ods_activity_info_full partition (dt='2020-06-14');