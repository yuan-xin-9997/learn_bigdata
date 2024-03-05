#! /bin/bash
# dws_1d_to_dws_nd.sh all/表名 [日期]

# 1. 判断参数是否传入
if [ $# -lt 1 ];then
    echo "必须传入all/表名 [日期]"
    exit 1
fi

# 2. 判断日期是否参数，如果传入则加载指定日期的数据，如果没有传入则加载前一天日期数据
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

dws_trade_user_sku_order_nd_sql="
insert overwrite table dws_trade_user_sku_order_nd partition (dt='$datestr')
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
        sum(if(dt>=date_sub('$datestr', 6), order_count_1d, 0)) order_count_7d,
        sum(if(dt>=date_sub('$datestr', 6), order_num_1d, 0)) order_num_7d,
        sum(if(dt>=date_sub('$datestr', 6), order_original_amount_1d, 0)) order_original_amount_7d,
        sum(if(dt>=date_sub('$datestr', 6), activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
        sum(if(dt>=date_sub('$datestr', 6), coupon_reduce_amount_1d, 0)) coupon_reduce_amount_7d,
        sum(if(dt>=date_sub('$datestr', 6), order_total_amount_1d, 0)) order_total_amount_7d,

        sum(order_count_1d) order_count_30d,
        sum(order_num_1d) order_num_30d,
        sum(order_original_amount_1d) order_original_amount_30d,
        sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
        sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
        sum(order_total_amount_1d) order_total_amount_30d

from dws_trade_user_sku_order_1d
where dt <= '$datestr'
  and dt >= date_sub('$datestr', 29)
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id,
         category3_name,
         tm_id, tm_name
;
"

dws_trade_province_order_nd_sql="
insert into table dws_trade_province_order_nd partition (dt='$datestr')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('$datestr',-6),order_count_1d,0)) order_count_7d,
    sum(if(dt>=date_add('$datestr',-6),order_original_amount_1d,0)) order_original_amount_7d,
    sum(if(dt>=date_add('$datestr',-6),activity_reduce_amount_1d,0)) activity_reduce_amount_7d,
    sum(if(dt>=date_add('$datestr',-6),coupon_reduce_amount_1d,0)) coupon_reduce_amount_7d,
    sum(if(dt>=date_add('$datestr',-6),order_total_amount_1d,0)) order_total_amount_7d,
    sum(order_count_1d) order_count_30d,
    sum(order_original_amount_1d) order_original_amount_30d,
    sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
    sum(order_total_amount_1d) order_total_amount_30d
from dws_trade_province_order_1d
where dt>=date_add('$datestr',-29)
and dt<='$datestr'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
"

# 3. 根据输入参数加载指定日期数据
case $1 in
"all")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict;${dws_trade_user_sku_order_nd_sql};${dws_trade_province_order_nd_sql}"
;;
"dws_trade_user_sku_order_nd")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_user_sku_order_nd_sql}"
;;
"dws_trade_province_order_nd")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_province_order_nd_sql}"
;;
esac