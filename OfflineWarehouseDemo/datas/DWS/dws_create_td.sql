-- 10.3 历史至今汇总表
-- 10.3.1 交易域用户粒度订单历史至今汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户id',
    `order_date_first`          STRING COMMENT '首次下单日期',
    `order_date_last`           STRING COMMENT '末次下单日期',
    `order_count_td`            BIGINT COMMENT '下单次数',
    `order_num_td`              BIGINT COMMENT '购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 业务过程：订单
-- 粒度：用户
-- 数据来源：从第一天数据到今天

-- （1）首日装载
insert overwrite table dws_trade_user_order_td partition (dt='2020-06-14')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from dws_trade_user_order_1d
where dt<='2020-06-14'
group by user_id
;


-- （2）每日装载
-- 方案一：直接查询历史至今所有数据，再次汇总。与首日加载一样
--       缺点：需要查询历史到目前日期的所有数据，数据量可能会比较大，计算会比较慢
--       优点：简单
insert overwrite table dws_trade_user_order_td partition (dt='2020-06-15')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from dws_trade_user_order_1d
where dt<='2020-06-15'
group by user_id
;

-- 方案二：将td表前一天分区结果 + 当日1d表结果
with old_agg as (
    select user_id,
           order_date_first,
           order_date_last,
           order_count_td,
           order_num_td,
           original_amount_td,
           activity_reduce_amount_td,
           coupon_reduce_amount_td,
           total_amount_td,
           dt
    from dws_trade_user_order_td where dt=date_sub('2020-06-15', 1)
), curr_agg as (
    select user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d,
           dt
    from dws_trade_user_order_1d where dt='2020-06-15'
)
insert overwrite table dws_trade_user_order_td partition (dt='2020-06-15')
select
    nvl(oa.user_id, ca.user_id),
    `if`(oa.user_id is not null, oa.order_date_first, '2020-06-15'),
    `if`(ca.user_id is not null, '2020-06-15', oa.order_date_last),
    nvl(oa.order_count_td, 0) + nvl(ca.order_count_1d, 0),
    nvl(oa.order_num_td, 0) + nvl(ca.order_num_1d, 0),
    nvl(oa.original_amount_td, 0.0) + nvl(ca.order_original_amount_1d, 0),
    nvl(oa.activity_reduce_amount_td, 0.0) + nvl(ca.activity_reduce_amount_1d, 0),
    nvl(oa.coupon_reduce_amount_td, 0.0) + nvl(ca.coupon_reduce_amount_1d, 0),
    nvl(oa.total_amount_td, 0.0) + nvl(ca.order_original_amount_1d, 0)
from old_agg oa full join curr_agg ca
    on oa.user_id=ca.user_id
;