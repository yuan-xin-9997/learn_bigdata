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

-- 方案二：将td表前一天分区结果 + 当日1d表结果(join)
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


-- 方案二：将td表前一天分区结果 + 当日1d表结果(union)
with old_agg as (
    select user_id,
           order_date_first,
           order_date_last,
           order_count_td,
           order_num_td,
           original_amount_td,
           activity_reduce_amount_td,
           coupon_reduce_amount_td,
           total_amount_td
    from dws_trade_user_order_td where dt=date_sub('2020-06-15', 1)
), curr_agg as (
    select user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d,
           '2020-06-15' order_date_first,
           '2020-06-15' order_date_last
    from dws_trade_user_order_1d where dt='2020-06-15'
), full_agg as (
    select
        user_id,
           order_date_first,
           order_date_last,
           order_count_td,
           order_num_td,
           original_amount_td,
           activity_reduce_amount_td,
           coupon_reduce_amount_td,
           total_amount_td
    from old_agg
    union
    select
        user_id,
           order_date_first,
           order_date_last,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d
    from curr_agg
)
insert overwrite table dws_trade_user_order_td partition (dt='2020-06-15')
select
    user_id,
    min(order_date_first),
    max(order_date_last),
    sum(order_count_td),
       sum(    order_num_td),
       sum(    original_amount_td),
       sum(    activity_reduce_amount_td),
       sum(    coupon_reduce_amount_td),
       sum(    total_amount_td)
from full_agg
group by user_id
;


-- 10.3.3 用户域用户粒度登录历史至今汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`         STRING COMMENT '用户id',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 2）数据装载
-- 业务过程：用户登录
    -- 用户域用户登录事务事实表是根据日志数据获取登录行为，日志没有历史数据，所以通过用户登录事实表拿不到所有用户登录数据，只能
    --      拿到首日之后的用户登录数据
    -- 要想获取首日之前的用户登录数据，只能从维度表获取
    --      1. 对于注册之后没有修改的用户并且首日之后没有再登录的，指定首次登录时间是注册时间，最后登录时间是注册时间
    --      2. 对于注册之后没修改的用户并且首日之后没有再登录的，指定首次登录时间是注册时间，最后登录时间是注册时间
-- （1）首日装载
with full_user as (select id,
                          date_format(create_time, 'yyyy-MM-dd') date_id
                   from dim_user_zip
                   where dt = '9999-12-31'
                     and date_format(create_time, 'yyyy-MM-dd') <= '2020-06-14'), login_user as (
select
    user_id,
    date_id
from dwd_user_login_inc where dt='2020-06-14'
), full_login_user as (
    select id,
           date_id
    from full_user
    union all
    select user_id, date_id
    from login_user
)
insert overwrite table dws_user_user_login_td partition (dt='2020-06-14')
select
    id,
    -- min(date_id) login_date_first,
    max(date_id) login_date_last,
    count(1) login_count_td
from full_login_user
group by id
;

-- （2）每日装载
with before_agg as (
    select user_id,
           login_date_last,
           login_count_td
    from dws_user_user_login_td
    where dt=date_sub('2020-06-15', 1)
), curr_agg as (
    select
        user_id,
        dt,
        1 login_count -- 事实表一行数据代表登录一次
    from dwd_user_login_inc where dt='2020-06-15'
), full_user as (
    select user_id,
           login_date_last,
           login_count_td
    from  before_agg
    union all
    select user_id,
           dt,
           login_count
    from curr_agg
)
insert overwrite table dws_user_user_login_td partition (dt='2020-06-15')
select user_id,
       max(login_date_last),
       sum(login_count_td)
from full_user
group by user_id
;