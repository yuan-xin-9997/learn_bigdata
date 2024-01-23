-- 日志文件中的单条日志数据格式
-- 我们的日志结构大致可分为两类，一是普通页面埋点日志，二是启动日志。
-- 普通页面日志结构如下，每条日志包含了，当前页面的页面信息，
-- 所有事件（动作）、所有曝光信息以及错误信息。除此之外，还包含了一系列公共信息，包括设备信息，地理位置，应用信息等
-- 格式1：
-- {
--     "actions": [
--         {
--             "action_id": "get_coupon",
--             "item": "2",
--             "item_type": "coupon_id",
--             "ts": 1592105356951
--         }
--     ],
--     "common": {
--         "ar": "370000",
--         "ba": "Huawei",
--         "ch": "oppo",
--         "is_new": "0",
--         "md": "Huawei P30",
--         "mid": "mid_228665",
--         "os": "Android 11.0",
--         "uid": "644",
--         "vc": "v2.1.134"
--     },
--     "displays": [
--         {
--             "display_type": "query",
--             "item": "5",
--             "item_type": "sku_id",
--             "order": 1,
--             "pos_id": 5
--         },
--         {
--             "display_type": "recommend",
--             "item": "31",
--             "item_type": "sku_id",
--             "order": 2,
--             "pos_id": 4
--         },
--         {
--             "display_type": "query",
--             "item": "19",
--             "item_type": "sku_id",
--             "order": 3,
--             "pos_id": 3
--         },
--         {
--             "display_type": "query",
--             "item": "20",
--             "item_type": "sku_id",
--             "order": 4,
--             "pos_id": 5
--         },
--         {
--             "display_type": "query",
--             "item": "19",
--             "item_type": "sku_id",
--             "order": 5,
--             "pos_id": 2
--         },
--         {
--             "display_type": "recommend",
--             "item": "11",
--             "item_type": "sku_id",
--             "order": 6,
--             "pos_id": 4
--         },
--         {
--             "display_type": "promotion",
--             "item": "4",
--             "item_type": "sku_id",
--             "order": 7,
--             "pos_id": 2
--         },
--         {
--             "display_type": "promotion",
--             "item": "27",
--             "item_type": "sku_id",
--             "order": 8,
--             "pos_id": 4
--         },
--         {
--             "display_type": "promotion",
--             "item": "25",
--             "item_type": "sku_id",
--             "order": 9,
--             "pos_id": 4
--         },
--         {
--             "display_type": "query",
--             "item": "15",
--             "item_type": "sku_id",
--             "order": 10,
--             "pos_id": 1
--         }
--     ],
--     "page": {
--         "during_time": 1902,
--         "item": "28",
--         "item_type": "sku_id",
--         "last_page_id": "good_list",
--         "page_id": "good_detail",
--         "source_type": "query"
--     },
--     "err": {
--         "error_code": 3795,
--         "msg": " Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)"
--     }
--     "ts": 1592105356000
-- }

-- 格式2：
-- {
--     "common": {
--         "ar": "230000",
--         "ba": "Redmi",
--         "ch": "oppo",
--         "is_new": "1",
--         "md": "Redmi k30",
--         "mid": "mid_531554",
--         "os": "Android 11.0",
--         "uid": "20",
--         "vc": "v2.1.134"
--     },
--     "start": {
--         "entry": "icon",
--         "loading_time": 6622,
--         "open_ad_id": 2,
--         "open_ad_ms": 5518,
--         "open_ad_skip_ms": 0
--     },
--     "err": {
--         "error_code": 3795,
--         "msg": " Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)"
--     }
--     "ts": 1592105351000
-- }


DROP TABLE IF EXISTS ods_log_inc;
CREATE EXTERNAL TABLE ods_log_inc
(
    common struct<ar:string, ba:string, ch:string, is_new:string, md:string, os:string, uid:string, vc:string>,
    actions array<struct<action_id:string, item:string, item_type:string, ts:bigint>>,
    display array<struct<display_type:string, item:string, item_type:string, `order`:int, pos_id: int >>,
    page struct<during_time:int, item:string, item_type:string, last_page_id:string, page_id: string, source_type:string>,
    `start` struct<entry:string, loading_time:int, open_ad_id:int, open_ad_ms: int, open_ad_skip_ms:int>,
    err struct<error_code:int, msg:string>,
    ts bigint
)
partitioned by(dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
location '/warehouse/gmall/ods/ods_log_inc/';

-- 加载数据到指定分区
-- load data inpath '/origin_data/gmall/log/topic_log/2020-06-14' overwrite into table ods_log_inc partition (dt='2020-06-14');
