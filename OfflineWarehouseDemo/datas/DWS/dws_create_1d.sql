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

-- 10.1.2 交易域用户粒度订单最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户id',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    dt
from dwd_trade_order_detail_inc
group by user_id,dt;
-- （2）每日装载
insert overwrite table dws_trade_user_order_1d partition(dt='2020-06-15')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt='2020-06-15'
group by user_id;

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


-- 10.1.5 交易域省份粒度订单最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_trade_province_order_1d;
CREATE EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '用户id',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码',
    `iso_3166_2`                STRING COMMENT '新版版ISO-3166-2编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 派生指标
-- 原子指标
--      业务过程：下单
--      度量值：下单次数、下单原始金额、下单活动优惠券金额、下单最终金额
--      聚合逻辑：count(distinct order_id), sum(..), sum(...), sum(...)
-- 统计周期：最近1日
-- 业务限定：无
-- 统计粒度：省份
-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition (dt)
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
from (select province_id,
             count(distinct order_id)           order_count_1d,
             sum(split_original_amount)         order_original_amount_1d,
             sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
             sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
             sum(split_total_amount)            order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt <= '2020-06-14'
      group by province_id, dt) t1
         left join (select id,
                           province_name,
                           area_code,
                           iso_code,
                           iso_3166_2,
                           region_id,
                           region_name,
                           dt
                    from dim_province_full
                    where dt = '2020-06-14') t2 on t1.province_id = t2.id
;

-- （2）每日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition (dt = '2020-06-15')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (select province_id,
             count(distinct order_id)           order_count_1d,
             sum(split_original_amount)         order_original_amount_1d,
             sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
             sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
             sum(split_total_amount)            order_total_amount_1d
      from dwd_trade_order_detail_inc
      where dt = '2020-06-15'
      group by province_id, dt) t1
         left join (select id,
                           province_name,
                           area_code,
                           iso_code,
                           iso_3166_2,
                           region_id,
                           region_name,
                           dt
                    from dim_province_full
                    where dt = '2020-06-15') t2 on t1.province_id = t2.id
;

-- 10.1.6 工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
CREATE EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
(
    `user_id`          STRING COMMENT '用户id',
    `coupon_id`        STRING COMMENT '优惠券id',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `benefit_rule`     STRING COMMENT '优惠规则',
    `used_count_1d`    STRING COMMENT '使用(支付)次数'
) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 派生指标
-- 原子指标
--      业务过程：优惠券使用(支付)
--      度量值：使用(支付)次数
--      聚合逻辑：count(1)
-- 统计周期：最近1日
-- 业务限定：无
-- 统计粒度：用户、优惠券
-- （1）首日装载
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt)
select user_id,
       coupon_id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       used_count_1d,
       dt
from (select user_id,
             coupon_id,
             count(1) used_count_1d,
             dt
      from dwd_tool_coupon_used_inc
      where dt <= '2020-06-14'
      group by user_id, coupon_id, dt) t1
         left join (select id,
                           coupon_name,
                           coupon_type_code,
                           coupon_type_name,
                           condition_amount,
                           condition_num,
                           activity_id,
                           benefit_amount,
                           benefit_discount,
                           benefit_rule
                    from dim_coupon_full
                    where dt = '2020-06-14') t2 on t1.coupon_id = t2.id;

-- （2）每日装载
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt = '2020-06-15')
select user_id,
       coupon_id,
       coupon_name,
       coupon_type_code,
       coupon_type_name,
       benefit_rule,
       used_count_1d
from (select user_id,
             coupon_id,
             count(1) used_count_1d,
             dt
      from dwd_tool_coupon_used_inc
      where dt = '2020-06-15'
      group by user_id, coupon_id, dt) t1
         left join (select id,
                           coupon_name,
                           coupon_type_code,
                           coupon_type_name,
                           condition_amount,
                           condition_num,
                           activity_id,
                           benefit_amount,
                           benefit_discount,
                           benefit_rule
                    from dim_coupon_full
                    where dt = '2020-06-15') t2 on t1.coupon_id = t2.id;


-- 10.1.7 互动域商品粒度收藏商品最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_interaction_sku_favor_add_1d;
CREATE EXTERNAL TABLE dws_interaction_sku_favor_add_1d
(
    `sku_id`             STRING COMMENT 'sku id',
    `sku_name`           STRING COMMENT 'sku名称',
    `category1_id`       STRING COMMENT '一级分类id',
    `category1_name`     STRING COMMENT '一级分类名称',
    `category2_id`       STRING COMMENT '一级分类id',
    `category2_name`     STRING COMMENT '一级分类名称',
    `category3_id`       STRING COMMENT '一级分类id',
    `category3_name`     STRING COMMENT '一级分类名称',
    `tm_id`              STRING COMMENT '品牌id',
    `tm_name`            STRING COMMENT '品牌名称',
    `favor_add_count_1d` BIGINT COMMENT '商品被收藏次数'
) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_interaction_sku_favor_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 派生指标
-- 原子指标
--      业务过程：收藏
--      度量值：收藏次数
--      聚合逻辑：count(1)
-- 统计周期：最近1日
-- 业务限定：无
-- 统计粒度：商品
-- （1）首日装载
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt)
select sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       favor_add_count_1d,
       dt
from (select sku_id,
             count(1) favor_add_count_1d,
             dt
      from dwd_interaction_favor_add_inc
      where dt <= '2020-06-14'
      group by sku_id, dt) t1
         left join (select id,
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
                    where dt = '2020-06-14') t2 on t1.sku_id = t2.id;

-- （2）每日装载
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt='2020-06-15')
select sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       favor_add_count_1d
from (select sku_id,
             count(1) favor_add_count_1d,
             dt
      from dwd_interaction_favor_add_inc
      where dt = '2020-06-15'
      group by sku_id, dt) t1
         left join (select id,
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
                    where dt = '2020-06-15') t2 on t1.sku_id = t2.id;

-- 10.1.8 流量域会话粒度页面浏览最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话id',
    `mid_id`         string comment '设备id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `version_code`   string comment 'app版本号',
    `channel`        string comment '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日访问时长',
    `page_count_1d`  BIGINT COMMENT '最近1日访问页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 派生指标
    -- 原子指标
    --      业务过程：页面浏览
    --      度量值：during_time，1
    --      聚合逻辑：sum(...), count(1)
    -- 统计周期：最近1日
    -- 业务限定：无
    -- 统计粒度：会话
-- 页面访问没有历史数据，所以每日访问与首日访问数据加载均相同
insert overwrite table dws_traffic_session_page_view_1d partition (dt='2020-06-14')
select
    session_id,mid_id,brand,model,operate_system,version_code,channel,
    sum(during_time),
    count(1)
from dwd_traffic_page_view_inc where dt='2020-06-14' -- 页面浏览只有首日的，没有历史
                               -- 一个session只会有一个设备id，手机品牌，手机型号，操作系统、app版本号
group by session_id,mid_id,brand,model,operate_system,version_code,channel;

-- 10.1.9 流量域访客页面粒度页面浏览最近1日汇总表
-- 1）建表语句
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客id',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面id',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 2）数据装载
-- 派生指标
    -- 原子指标
    --      业务过程：页面浏览
    --      度量值：during_time，1
    --      聚合逻辑：sum(...), count(1)
    -- 统计周期：最近1日
    -- 业务限定：无
    -- 统计粒度：访客、页面
-- 页面访问没有历史数据，所以每日访问与首日访问数据加载均相同
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt='2020-06-14')
select
    mid_id,brand,model,operate_system,page_id,
    sum(during_time),count(1)
from dwd_traffic_page_view_inc where dt='2020-06-14'
group by mid_id,brand,model,operate_system,page_id;














