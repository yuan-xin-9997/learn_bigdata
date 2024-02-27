-- DWS 层建表脚本
-- 表命名规范：dws_作用域_粒度_业务过程_1d/nd/td

-- 指标：最近1日各品牌的订单数、订单人数
create table dws_trade_tm_order_1d
(
    tm_id            string,
    tm_name          string,
    order_count      bigint,
    order_user_count bigint
) partitioned by (`dt` STRING) stored as orc LOCATION '/warehouse/gmall/dws/dws_trade_tm_order_1d';

-- 数据加载
insert overwrite table dws_trade_tm_order_1d partition (dt='2020-06-14')
select
    tm_id,
    tm_name,
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
group by tm_id, tm_name
;