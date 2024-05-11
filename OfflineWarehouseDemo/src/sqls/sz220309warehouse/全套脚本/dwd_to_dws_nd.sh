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


dws_trade_province_order_nd="
insert overwrite table dws_trade_province_order_nd partition (dt='$do_date')
select
        province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2,
        sum(\\`if\\`(dt > date_sub('$do_date',7) ,order_count_1d,0)) order_count_7d,
        sum(\\`if\\`(dt > date_sub('$do_date',7) ,order_original_amount_1d,0)) order_original_amount_7d,
        sum(\\`if\\`(dt > date_sub('$do_date',7) ,activity_reduce_amount_1d,0)) activity_reduce_amount_7d,
        sum(\\`if\\`(dt > date_sub('$do_date',7) ,coupon_reduce_amount_1d,0)) coupon_reduce_amount_7d,
        sum(\\`if\\`(dt > date_sub('$do_date',7) ,order_total_amount_1d,0)) order_total_amount_7d,
        sum(order_count_1d) order_count_30d,
        sum(order_original_amount_1d) order_original_amount_30d,
        sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
        sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
        sum(order_total_amount_1d) order_total_amount_30d
from dws_trade_province_order_1d
where dt > date_sub('$do_date',30)
group by province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2;
"


dws_trade_user_sku_order_nd="
insert overwrite table dws_trade_user_sku_order_nd partition (dt='$do_date')
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
                -- 统计最近7天的指标，先判断当前这行数据的dt是否符合时间范围，符合就统计，不符合记为0|null再统计
               sum(\\`if\\`(dt > date_sub('$do_date',7) , order_count_1d , 0)) order_count_7d,
               sum(\\`if\\`(dt > date_sub('$do_date',7) , order_num_1d , 0)) order_num_7d,
               sum(\\`if\\`(dt > date_sub('$do_date',7) , order_original_amount_1d , 0)) order_original_amount_7d,
               sum(\\`if\\`(dt > date_sub('$do_date',7) , activity_reduce_amount_1d , 0)) activity_reduce_amount_7d,
               sum(\\`if\\`(dt > date_sub('$do_date',7) , coupon_reduce_amount_1d , 0)) coupon_reduce_amount_7d,
               sum(\\`if\\`(dt > date_sub('$do_date',7) , order_total_amount_1d , 0)) order_total_amount_7d,
                sum(order_count_1d)            order_count_30d,
                sum(order_num_1d)              order_num_30d,
                sum(order_original_amount_1d)  order_original_amount_30d,
                sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
                sum(coupon_reduce_amount_1d)   coupon_reduce_amount_30d,
                sum(order_total_amount_1d)     order_total_amount_30d
-- 一个用户在一天中购买一种sku的统计值
         from dws_trade_user_sku_order_1d
         where dt > date_sub('$do_date', 30)
         group by user_id,
                  sku_id,
                  sku_name,
                  category1_id,
                  category1_name,
                  category2_id,
                  category2_name,
                  category3_id,
                  category3_name,
                  tm_id,
                  tm_name;
"


case $1 in
    "dws_trade_province_order_nd" )
        hive --database gmall -e "$dws_trade_province_order_nd"
    ;;
    "dws_trade_user_sku_order_nd" )
        hive --database gmall -e "$dws_trade_user_sku_order_nd"
    ;;
    "all" )
        hive --database gmall -e "$dws_trade_province_order_nd$dws_trade_user_sku_order_nd"
    ;;
esac
