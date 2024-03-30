#!/bin/bash
if [ -n "$2" ]
then
	do_date=$2
else
	#echo 必须给我传一个日期
	#exit
	do_date= $(date -d yesterday +%F) 
fi

echo 当前要操作的日期是$do_date


dws_trade_province_order_1d="
insert overwrite table dws_trade_province_order_1d partition (dt='$do_date')
select
     province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (
         select province_id,
                count(distinct order_id)   order_count_1d,
                sum(split_original_amount) order_original_amount_1d,
                sum(split_activity_amount) activity_reduce_amount_1d,
                sum(split_coupon_amount)   coupon_reduce_amount_1d,
                sum(split_total_amount)    order_total_amount_1d
-- 一笔单中的一个成功被支付的商品是一行
         from dwd_trade_order_detail_inc
         where dt = '$do_date'
         group by province_id
     ) t1
left join
    (

          select
                province_name,
               area_code,
               iso_code,
               iso_3166_2,
                 id
            from dim_province_full
            where dt='$do_date'

        )t2
on t1.province_id = t2.id;
"
dws_trade_user_cart_add_1d="
insert overwrite table dws_trade_user_cart_add_1d partition (dt='$do_date')
    select
        user_id,
       count(*) cart_add_count_1d,
       sum(sku_num) cart_add_num_1d
     from dwd_trade_cart_add_inc
     where dt='$do_date'
    group by user_id;
"
dws_trade_user_order_1d="
insert overwrite table dws_trade_user_order_1d partition (dt='$do_date')
    select
        user_id,
           -- 下单次数 = 下单数
       count(distinct  order_id) order_count_1d,
       sum(sku_num) order_num_1d,
       sum(split_original_amount) order_original_amount_1d,
       sum(split_activity_amount) activity_reduce_amount_1d,
       sum(split_coupon_amount) coupon_reduce_amount_1d,
       sum(split_total_amount) order_total_amount_1d
    -- 粒度： 一个用户下的一笔订单的一种商品是一行
     from dwd_trade_order_detail_inc
     where dt='$do_date'
    group by user_id;
"

dws_trade_user_payment_1d="
insert overwrite table dws_trade_user_payment_1d partition (dt='$do_date')
    select
        user_id,
           -- 支付以订单为单位付款，不是以商品为单位付款
       count(distinct order_id) payment_count_1d,
       sum(sku_num) payment_num_1d,
       sum(split_payment_amount) payment_amount_1d
-- 一笔单中的一个成功被支付的商品是一行
     from dwd_trade_pay_detail_suc_inc
     where dt='$do_date'
    group by user_id;

"
dws_trade_user_sku_order_1d="
insert overwrite table dws_trade_user_sku_order_1d partition (dt='$do_date')
select
        -- 统计的粒度  group by 后面
       user_id,
       sku_id,
       -- 属于维度，是对sku_id的补充说明，从维度表中关联
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       -- 属于要聚合的字段，从事实表中聚合
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
from (-- A  只能这样写  原则：  只要从分区表中查询数据，一定要加分区字段！即使是全表查询也要加分区字段
         select
             -- group by之后，select后面能写什么?
             -- 只能写  ①group by后的字段  ②聚合函数中可以写任意字段  ③常量
             user_id,
             sku_id,

             count(*)                   order_count_1d,
             sum(sku_num)               order_num_1d,
             sum(split_original_amount) order_original_amount_1d,
             sum(split_activity_amount) activity_reduce_amount_1d,
             sum(split_coupon_amount)   coupon_reduce_amount_1d,
             sum(split_total_amount)    order_total_amount_1d
             -- 粒度：  一个人在一天中购买的一笔订单的一种商品是一行
         from dwd_trade_order_detail_inc
         where dt = '$do_date'
         group by user_id, sku_id
     ) t1
left join
(
      select
             sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
             id
        from dim_sku_full
        where dt='$do_date'

    )t2
on t1.sku_id = t2.id;
"

dws_traffic_page_visitor_page_view_1d="
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt='$do_date')
  select
       mid_id,
       brand,
       model,
       operate_system,
       page_id,
        sum(during_time) during_time_1d,
        count(*) view_count_1d
  -- 粒度： 一个页面是一行
    from dwd_traffic_page_view_inc
    where dt='$do_date'
    group by mid_id,page_id,
              brand,
               model,
               operate_system;
"
dws_traffic_session_page_view_1d="
insert overwrite table dws_traffic_session_page_view_1d partition (dt='$do_date')
  select
        session_id,
          mid_id,
               brand,
               model,
               operate_system,
               version_code,
               channel,
        sum(during_time) during_time_1d,
        count(*) page_count_1d
  -- 粒度： 一个页面是一行
    from dwd_traffic_page_view_inc
    where dt='$do_date'
    group by session_id,
             mid_id,
               brand,
               model,
               operate_system,
               version_code,
               channel;
"

dws_tool_user_coupon_coupon_used_1d="
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(dt='$do_date')
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count
from
(
    select
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    where dt='$do_date'
    group by user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where dt='$do_date'
)t2
on t1.coupon_id=t2.id;
"

dws_interaction_sku_favor_add_1d="
insert overwrite table dws_interaction_sku_favor_add_1d partition(dt='$do_date')
select
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
    favor_add_count
from
(
    select
        sku_id,
        count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    where dt='$do_date'
    group by sku_id
)favor
left join
(
    select
        id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    from dim_sku_full
    where dt='$do_date'
)sku
on favor.sku_id=sku.id;

"

case $1 in
    "dws_trade_province_order_1d" )
        hive --database gmall -e "$dws_trade_province_order_1d"
    ;;
    "dws_trade_user_cart_add_1d" )
        hive --database gmall -e "$dws_trade_user_cart_add_1d"
    ;;
    "dws_trade_user_order_1d" )
        hive --database gmall -e "$dws_trade_user_order_1d"
    ;;
    "dws_trade_user_payment_1d" )
        hive --database gmall -e "$dws_trade_user_payment_1d"
    ;;
    "dws_trade_user_sku_order_1d" )
        hive --database gmall -e "$dws_trade_user_sku_order_1d"
    ;;
    "dws_tool_user_coupon_coupon_used_1d" )
        hive --database gmall -e "$dws_tool_user_coupon_coupon_used_1d"
    ;;
    "dws_interaction_sku_favor_add_1d" )
        hive --database gmall -e "$dws_interaction_sku_favor_add_1d"
    ;;
    "dws_traffic_page_visitor_page_view_1d" )
        hive --database gmall -e "$dws_traffic_page_visitor_page_view_1d"
    ;;
    "dws_traffic_session_page_view_1d" )
        hive --database gmall -e "$dws_traffic_session_page_view_1d"
    ;;
    "all" )
        hive --database gmall -e "$dws_trade_province_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_order_1d$dws_trade_user_payment_1d$dws_trade_user_sku_order_1d$dws_tool_user_coupon_coupon_used_1d$dws_interaction_sku_favor_add_1d$dws_traffic_page_visitor_page_view_1d$dws_traffic_session_page_view_1d"
    ;;
esac

