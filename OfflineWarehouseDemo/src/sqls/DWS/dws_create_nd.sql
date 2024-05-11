-- 10.2 最近n日汇总表
-- 10.2.1 交易域用户商品粒度订单最近n日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                    STRING COMMENT '用户id',
    `sku_id`                     STRING COMMENT 'sku_id',
    `sku_name`                   STRING COMMENT 'sku名称',
    `category1_id`               STRING COMMENT '一级分类id',
    `category1_name`             STRING COMMENT '一级分类名称',
    `category2_id`               STRING COMMENT '一级分类id',
    `category2_name`             STRING COMMENT '一级分类名称',
    `category3_id`               STRING COMMENT '一级分类id',
    `category3_name`             STRING COMMENT '一级分类名称',
    `tm_id`                      STRING COMMENT '品牌id',
    `tm_name`                    STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 数据来源：dws_trade_user_sku_order_1d
-- 粒度：一行代表一个用户一个商品当天的统计结果
-- nd表一般不需要处理历史数据，所以每日与首日逻辑一样
insert overwrite table dws_trade_user_sku_order_nd partition (dt='2020-06-14')
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
        sum(`if`(dt>=date_sub('2020-06-14', 6), order_count_1d, 0)) order_count_7d,
        sum(`if`(dt>=date_sub('2020-06-14', 6), order_num_1d, 0)) order_num_7d,
        sum(`if`(dt>=date_sub('2020-06-14', 6), order_original_amount_1d, 0)) order_original_amount_7d,
        sum(`if`(dt>=date_sub('2020-06-14', 6), activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
        sum(`if`(dt>=date_sub('2020-06-14', 6), coupon_reduce_amount_1d, 0)) coupon_reduce_amount_7d,
        sum(`if`(dt>=date_sub('2020-06-14', 6), order_total_amount_1d, 0)) order_total_amount_7d,

        sum(order_count_1d) order_count_30d,
        sum(order_num_1d) order_num_30d,
        sum(order_original_amount_1d) order_original_amount_30d,
        sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
        sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
        sum(order_total_amount_1d) order_total_amount_30d

from dws_trade_user_sku_order_1d
where dt <= '2020-06-14'
  and dt >= date_sub('2020-06-14', 29)
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id,
         category3_name,
         tm_id, tm_name
;


-- 10.2.2 交易域省份粒度订单最近n日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '用户id',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                 STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载

insert into table dws_trade_province_order_nd partition (dt='2020-06-14')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('2020-06-14',-6),order_count_1d,0)) order_count_7d,
    sum(if(dt>=date_add('2020-06-14',-6),order_original_amount_1d,0)) order_original_amount_7d,
    sum(if(dt>=date_add('2020-06-14',-6),activity_reduce_amount_1d,0)) activity_reduce_amount_7d,
    sum(if(dt>=date_add('2020-06-14',-6),coupon_reduce_amount_1d,0)) coupon_reduce_amount_7d,
    sum(if(dt>=date_add('2020-06-14',-6),order_total_amount_1d,0)) order_total_amount_7d,
    sum(order_count_1d) order_count_30d,
    sum(order_original_amount_1d) order_original_amount_30d,
    sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
    sum(order_total_amount_1d) order_total_amount_30d
from dws_trade_province_order_1d
where dt>=date_add('2020-06-14',-29)
and dt<='2020-06-14'
group by province_id,province_name,area_code,iso_code,iso_3166_2;


















