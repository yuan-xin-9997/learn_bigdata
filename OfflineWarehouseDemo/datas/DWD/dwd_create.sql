-- 交易域加购事务事实表
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码（冗余）',
    `source_type_name` STRING COMMENT '来源类型名称（冗余）',
    `sku_num`          BIGINT COMMENT '加购物车件数（冗余）'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy')
;

-- 加载数据
---  首日加载：数据放入事实发生对应的日期分区中
---      分区：保存当日发生的事实
---      数据来源：ODS_cart_info_full[2020-06-14]
---      去处：根据加购物车的时间放入对应的分区中
set hive.exec.dynamic.partition.mode=nonstrict; -- 若要使用动态分区，需要设置hive参数为非严格模式
with cart as (
  select
        data.id,
        data.user_id,
        data.sku_id,
        -- 首日不知道加了几次购物车，以第一次加购物时间作为事件发生时间
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time,
        data.source_id,
        data.source_type,
        -- data.source_type_name, -- 从字典表查询
        data.sku_num
  from ods_cart_info_inc where dt='2020-06-14' and type='bootstrap-insert' -- 注意：此处不是full，full是后续做 周期型快照事实表的时候使用的
), dc as (
    select
        dic_name source_type_name,
        dic_code
    from ods_base_dic_full where dt='2020-06-14' and parent_code='24'
)
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type,
    source_type_name,
    sku_num,
    date_id -- 动态分区字段
from cart
left join dc
on cart.source_type = dc.dic_code;

---  每日加载：数据放入事实表当日分区
---      分区：保存当日发生的事实
---      数据来源：ODS_cart_info_full[指定日期]
---      去处：放入[指定日期]对应的分区中
with cart as (
    select
        data.id,
        data.user_id,
        data.sku_id,
        -- 事件时间，加购物车的时间，基于ts
        date_format(from_utc_timestamp(ts*1000, "Asia/Shanghai"), 'yyyy-MM-dd') date_id,
        -- 加购物车时间，精确到时分秒，基于ts
        date_format(from_utc_timestamp(ts*1000, "Asia/Shanghai"), 'yyyy-MM-dd HH:mm:ss') create_time,
        data.source_id,
        data.source_type,
        -- data.source_type_name, -- 从字典表查询
        data.sku_num
    from ods_cart_info_inc
    where dt='2020-06-15' and
          (type='insert' or
           (type='update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as bigint)) )
), bc as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-15' and parent_code='24'
)
insert overwrite  table dwd_trade_cart_add_inc partition (dt='2020-06-15')
select
    id,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type,
    source_type_name,
    sku_num -- 不需要动态分区
from cart left join bc on cart.source_type = bc.dic_code