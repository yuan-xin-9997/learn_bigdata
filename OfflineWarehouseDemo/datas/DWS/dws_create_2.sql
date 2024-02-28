-- 10.1.1 交易域用户商品粒度订单最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `sku_id`                    STRING COMMENT 'sku_id',
    `sku_name`                  STRING COMMENT 'sku名称',
    `category1_id`              STRING COMMENT '一级分类id',
    `category1_name`            STRING COMMENT '一级分类名称',
    `category2_id`              STRING COMMENT '一级分类id',
    `category2_name`            STRING COMMENT '一级分类名称',
    `category3_id`              STRING COMMENT '一级分类id',
    `category3_name`            STRING COMMENT '一级分类名称',
    `tm_id`                     STRING COMMENT '品牌id',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- （1）首日装载[10 11 12 13 14]，首日有历史数据数据需要处理，每日没有
set hive.exec.dynamic.partition.mode=nonstrict;
with od as (select user_id,
                   sku_id,
                   count(1)                           order_count_1d,
                   sum(sku_num)                       order_num_1d,
                   sum(split_original_amount)         order_original_amount_1d,
                   sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                   sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                   sum(split_total_amount)            order_total_amount_1d,
                   dt
            from dwd_trade_order_detail_inc
            where dt <= '2020-06-14'
            group by user_id, sku_id, dt),
     sku as (select id,
                    price,
                    sku_name,
                    sku_desc,
                    weight,
                    is_sale,
                    spu_id,
                    spu_name,
                    category3_id,
                    category3_name,
                    category2_id,
                    category2_name,
                    category1_id,
                    category1_name,
                    tm_id,
                    tm_name,
                    sku_attr_values,
                    sku_sale_attr_values,
                    create_time
             from dim_sku_full
             where dt = '2020-06-14')
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from od left join sku on od.sku_id = sku.id
;

-- 2) 每日加载
with od as (select user_id,
                   sku_id,
                   count(1)                           order_count_1d,
                   sum(sku_num)                       order_num_1d,
                   sum(split_original_amount)         order_original_amount_1d,
                   sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                   sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                   sum(split_total_amount)            order_total_amount_1d,
                   dt
            from dwd_trade_order_detail_inc
            where dt = '2020-06-15'
            group by user_id, sku_id, dt),
     sku as (select id,
                    price,
                    sku_name,
                    sku_desc,
                    weight,
                    is_sale,
                    spu_id,
                    spu_name,
                    category3_id,
                    category3_name,
                    category2_id,
                    category2_name,
                    category1_id,
                    category1_name,
                    tm_id,
                    tm_name,
                    sku_attr_values,
                    sku_sale_attr_values,
                    create_time
             from dim_sku_full
             where dt = '2020-06-15')
insert overwrite table dws_trade_user_sku_order_1d partition (dt='2020-06-15')
select user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from od left join sku on od.sku_id = sku.id
;


-- 10.1.3 交易域用户粒度加购最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 2）数据装载
 -- 派生指标
    -- 原子指标
    --      业务过程：加购
    --      度量值：次数、商品件数
    --      聚合逻辑：count(1), sum(..)
    -- 统计周期：最近1日
    -- 业务限定：无
    -- 统计粒度：用户
-- （1）首日装载 14 需要对历史数据统计处理
insert overwrite table dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(1) cart_add_count_1d,
    sum(sku_num) cart_add_num_1d,
    dt
from dwd_trade_cart_add_inc
where dt<='2020-06-14'
group by user_id,dt;

-- （2）每日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '2020-06-15')
select user_id,
       count(1)     cart_add_count_1d,
       sum(sku_num) cart_add_num_1d
from dwd_trade_cart_add_inc
where dt = '2020-06-15'
group by user_id;


-- 10.1.4 交易域用户粒度支付最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户id',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 2）数据装载
 -- 派生指标
    -- 原子指标
    --      业务过程：支付
    --      度量值：支付次数、支付商品件数、支付商品件数
    --      聚合逻辑：count(distinct order_id), sum(..), sum(...)
    -- 统计周期：最近1日
    -- 业务限定：无
    -- 统计粒度：用户
-- （1）首日装载
insert overwrite table dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)) payment_count_1d,
    sum(sku_num) payment_num_1d,
    sum(split_payment_amount) payment_amount_1d,
    dt
from dwd_trade_pay_detail_suc_inc
where dt<='2020-06-14'
group by user_id,dt;

-- （2）每日装载
insert overwrite table dws_trade_user_payment_1d partition(dt='2020-06-15')
select
    user_id,
    count(distinct(order_id)) payment_count_1d,
    sum(sku_num) payment_num_1d,
    sum(split_payment_amount) payment_amount_1d
from dwd_trade_pay_detail_suc_inc
where dt='2020-06-15'
group by user_id;























