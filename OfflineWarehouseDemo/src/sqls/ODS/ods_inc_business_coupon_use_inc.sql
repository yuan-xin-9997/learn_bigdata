-- ODS层增量业务表建立  ods_coupon_use_inc

-- {
--     "database": "gmall",
--     "table": "coupon_use",
--     "type": "bootstrap-insert",
--     "ts": 1592105623,
--     "data": {
--         "id": 49885,
--         "coupon_id": 3,
--         "user_id": 88,
--         "order_id": 5498,
--         "coupon_status": "1403",
--         "create_time": null,
--         "get_time": "2020-06-14 11:33:22",
--         "using_time": "2020-06-14 11:33:22",
--         "used_time": "2020-06-14 11:33:23",
--         "expire_time": null
--     }
-- }


-- {
--     "database": "gmall",
--     "table": "coupon_use",
--     "type": "update",
--     "ts": 1592192354,
--     "xid": 25080,
--     "xoffset": 2556,
--     "data": {
--         "id": 49859,
--         "coupon_id": 3,
--         "user_id": 62,
--         "order_id": 5572,
--         "coupon_status": "1403",
--         "create_time": null,
--         "get_time": "2020-06-14 11:33:22",
--         "using_time": "2020-06-15 11:39:13",
--         "used_time": "2020-06-15 11:39:14",
--         "expire_time": null
--     },
--     "old": {
--         "coupon_status": "1402",
--         "used_time": null
--     }
-- }


DROP TABLE IF EXISTS ods_coupon_use_inc;
CREATE EXTERNAL TABLE ods_coupon_use_inc(
    type string,
    ts bigint,
    data struct<id:bigint, coupon_id:bigint, user_id:bigint,
                order_id:bigint, coupon_status:string, create_time:string,
                get_time:string, using_time:string, used_time:string, expire_time:string>,
    old map<string, string>
)
partitioned by(dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
location '/warehouse/gmall/ods/ods_coupon_use_inc/';

-- 加载数据
load data inpath '/origin_data/gmall/db/coupon_use_inc/2020-06-14' overwrite into table ods_coupon_use_inc partition (dt='2020-06-14');