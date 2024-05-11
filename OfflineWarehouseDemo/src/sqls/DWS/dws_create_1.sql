-- DWS 层建表脚本
-- 表命名规范：dws_作用域_粒度_业务过程_1d/nd/td

-- 指标：最近1日各用户-品牌的订单数、订单人数
drop table dws_trade_tm_order_1d;
create table dws_trade_tm_order_1d
(
    tm_id            string,
    tm_name          string,
    user_id string,
    order_count      bigint,
    order_user_count bigint
) partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_tm_order_1d';

-- 数据加载
insert overwrite table dws_trade_tm_order_1d partition (dt='2020-06-14')
select
    tm_id,
    tm_name,
    user_id,
    count(1),
    count(distinct user_id)
from (select
          sku_id,
          user_id
      from dwd_trade_order_detail_inc where dt = '2020-06-14') od left join (
    select
        id,
        tm_id,
        tm_name
    from dim_sku_full where dt='2020-06-14'
) dk on od. sku_id=dk.id
group by tm_id, tm_name,user_id
;


-- 指标：最近1日各品类的订单数、订单人数
drop table dws_trade_category_order_nd;
create table dws_trade_category_order_nd
(
    category1_id     string,
    category1_name   string,
    category2_id     string,
    category2_name   string,
    category3_id     string,
    category3_name   string,
    user_id string,
    order_count_7d      bigint,
    order_count_30d     bigint
--     order_user_count bigint
) partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_category_order_1d';

-- 数据加载
insert overwrite table dws_trade_category_order_1d partition (dt = '2020-06-14')
select category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       count(1)                order_count,
       count(distinct user_id) order_user_count
from (select sku_id,
             user_id
      from dwd_trade_order_detail_inc
      where dt = '2020-06-14') od
         left join (select id,
                           category1_id,
                           category1_name,
                           category2_id,
                           category2_name,
                           category3_id,
                           category3_name
                    from dim_sku_full
                    where dt = '2020-06-14') sku on od.sku_id = sku.id
group by category1_id,
         category1_name,
         category2_id,
         category2_name,
         category3_id,
         category3_name;


-- 指标：最近N日用户-品牌的订单数、订单人数
-- 建表方式1（适用最近N日比较少的场景）
drop table dws_trade_tm_order_nd;
create table dws_trade_tm_order_nd
(
    tm_id            string,
    tm_name          string,
    user_id string,
    order_count_7d      bigint comment '最近7日下单数',
--     order_user_count_7d bigint comment '最近7日用户数',
    order_count_30d      bigint comment '最近30日下单数'
--     ,order_user_count_30d bigint comment '最近30日用户数'
) partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_tm_order_nd';
-- 建表方式2（适用最近N日不确定的场景）
-- create table dws_trade_tm_order_nd
-- (
--     recent_day  string  comment '最忌N日',
--     tm_id            string,
--     tm_name          string,
--     order_count      bigint comment '最近recent_day日下单数',
--     order_user_count bigint comment '最近recent_day日用户数'
-- ) partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_tm_order_1d';

-- 加载数据：不通过dwd层获取数据，直接从dws层最近1日表中加载数据
-- 最近7日（不需要操作，直接在最近30日中完成查询
-- select
--     tm_id,
--     tm_name,
--     sum(order_count),
--     sum(order_user_count)
-- from dws_trade_tm_order_1d where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 6)
-- group by tm_id, tm_name;

-- 最近30日
select
    tm_id,
    tm_name,
    user_id,
    sum(`if`(dt<='2020-06-14' and dt>date_sub('2020-06-14', 6), order_count, 0)) order_count_7d,
--     sum(`if`(dt<='2020-06-14' and dt>date_sub('2020-06-14', 6), order_user_count, 0)) order_user_count_7d,
    sum(order_count) order_count_30d
--     ,sum(order_user_count) order_user_count_30d
from dws_trade_tm_order_1d where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 29)
group by tm_id, tm_name,user_id;

select
    tm_id,
    tm_name,
    sum(order_count),
    count(1)
from dws_trade_tm_order_1d
group by tm_id,tm_name;

select
    tm_id,
    tm_name,
    sum(order_count_7d),
    sum(`if`(order_count_7d)>0, 1, 0)
from dws_trade_tm_order_nd
group by tm_id ,  tm_name;


-- 最近1日用户商品粒度
drop table dws_trade_user_sku_order_1d;
create table dws_trade_user_sku_order_1d(
    user_id string,
    sku_id string,
    tm_id string,
    tm_name string,
    category1_id     string,
    category1_name   string,
    category2_id     string,
    category2_name   string,
    category3_id     string,
    category3_name   string,
    order_count string
)
partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d';

insert overwrite table dws_trade_user_sku_order_1d partition (dt='2020-06-14')
select
    user_id ,
    sku_id ,
    tm_id ,
    tm_name ,
    category1_id     ,
    category1_name   ,
    category2_id     ,
    category2_name   ,
    category3_id     ,
    category3_name,
    count(1)
from (
select
    user_id,
    sku_id
from dwd_trade_order_detail_inc where dt='2020-06-14') t1
left join
    (
select id,
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
       create_time,
       dt
from dim_sku_full where dt='2020-06-14') t2 on t1.sku_id=t2.id
group by user_id, sku_id, tm_id, tm_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name

-- 最近N日用户商品粒度
drop table dws_trade_user_sku_order_nd;
create table dws_trade_user_sku_order_nd(
    user_id string,
    sku_id string,
    tm_id string,
    tm_name string,
    category1_id     string,
    category1_name   string,
    category2_id     string,
    category2_name   string,
    category3_id     string,
    category3_name   string,
    order_count_7d bigint,
    order_count_30d bigint
)
partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd';

-- 数据加载
insert overwrite table dws_trade_user_sku_order_nd partition (dt='2020-06-14')
select
    user_id, sku_id, tm_id,
    tm_name, category1_id, category1_name,
    category2_id, category2_name, category3_id,
    category3_name,
    sum(`if`(dt<='2020-06-14' and dt>=date_sub('2020-06-14', 6), order_count, 0)),
    sum(order_count)
from dws_trade_user_sku_order_1d where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 29)
group by user_id, sku_id, tm_id, tm_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
;

-- DWS层建表建议：
-- 1.将需求拆分成派生指标
-- 2.将最近1日、最近N日指标分为不同的表（后续最近N日的数据从最近1日表中查询加载，相比直接从DWD层加载N日数据，计算的数据量更少，性能更快，成本更低）
-- 3.将业务过程、统计周期、统计粒度相同的指标结果放入同一个汇总表中
-- 4.考虑N日直接从1日表中查询加载的时候可能出现的问题（此时可以考虑降低粒度）
-- 分拆多个相同业务过程的派生指标，将统计粒度有关联的放入一个表中（降低粒度，比如品牌、品类都属于商品的维度属性，此时可以将维度调整为商品）（目的为了让DWS层的表更加通用，适用于更多指标）


