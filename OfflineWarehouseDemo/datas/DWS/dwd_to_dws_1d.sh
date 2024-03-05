#! /bin/bash
# dwd_to_dws.sh all/表名 [日期]

# 1. 判断参数是否传入
if [ $# -lt 1 ];then
    echo "必须传入all/表名 [日期]"
    exit 1
fi

# 2. 判断日期是否参数，如果传入则加载指定日期的数据，如果没有传入则加载前一天日期数据
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)


# 3. 根据输入参数加载指定日期数据

dws_interaction_sku_favor_add_1d_sql="
insert overwrite table dws_interaction_sku_favor_add_1d partition (dt='$datestr')
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
      where dt = '$datestr'
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
                    where dt = '$datestr') t2 on t1.sku_id = t2.id;
"


dws_tool_user_coupon_coupon_used_1d_sql="
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition (dt = '$datestr')
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
      where dt = '$datestr'
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
                    where dt = '$datestr') t2 on t1.coupon_id = t2.id;
"


dws_trade_province_order_1d_sql="
insert overwrite table dws_trade_province_order_1d partition (dt = '$datestr')
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
      where dt = '$datestr'
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
                    where dt = '$datestr') t2 on t1.province_id = t2.id
;
"


dws_trade_user_cart_add_1d_sql="
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '$datestr')
select user_id,
       count(1)     cart_add_count_1d,
       sum(sku_num) cart_add_num_1d
from dwd_trade_cart_add_inc
where dt = '$datestr'
group by user_id;
"


dws_trade_user_sku_order_1d_sql="

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
            where dt = '$datestr'
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
             where dt = '$datestr')
insert overwrite table dws_trade_user_sku_order_1d partition (dt='$datestr')
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
"


dws_trade_user_payment_1d_sql="
insert overwrite table dws_trade_user_payment_1d partition(dt='$datestr')
select
    user_id,
    count(distinct(order_id)) payment_count_1d,
    sum(sku_num) payment_num_1d,
    sum(split_payment_amount) payment_amount_1d
from dwd_trade_pay_detail_suc_inc
where dt='$datestr'
group by user_id;
"


dws_traffic_page_visitor_page_view_1d_sql="
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt='$datestr')
select
    mid_id,brand,model,operate_system,page_id,
    sum(during_time),count(1)
from dwd_traffic_page_view_inc where dt='$datestr'
group by mid_id,brand,model,operate_system,page_id;
"


dws_traffic_session_page_view_1d_sql="
insert overwrite table dws_traffic_session_page_view_1d partition (dt='$datestr')
select
    session_id,mid_id,brand,model,operate_system,version_code,channel,
    sum(during_time),
    count(1)
from dwd_traffic_page_view_inc where dt='$datestr' -- 页面浏览只有首日的，没有历史
                               -- 一个session只会有一个设备id，手机品牌，手机型号，操作系统、app版本号
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
"

dws_trade_user_order_1d_sql="
insert overwrite table dws_trade_user_order_1d partition(dt='$datestr')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt='$datestr'
group by user_id;
"


case $1 in
"all")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict;${dws_trade_user_order_1d_sql};${dws_interaction_sku_favor_add_1d_sql};${dws_tool_user_coupon_coupon_used_1d_sql};${dws_trade_province_order_1d_sql}; ${dws_trade_user_cart_add_1d_sql}; ${dws_trade_user_sku_order_1d_sql}; ${dws_trade_user_payment_1d_sql}; ${dws_traffic_page_visitor_page_view_1d_sql};${dws_traffic_session_page_view_1d_sql}"
;;
"dws_interaction_sku_favor_add_1d")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_interaction_sku_favor_add_1d_sql}"
;;
"dws_trade_user_order_1d")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_user_order_1d_sql}"
;;
"dws_tool_user_coupon_coupon_used_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_tool_user_coupon_coupon_used_1d_sql}"
;;
"dws_trade_province_order_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_province_order_1d_sql}"
;;
"dws_trade_user_cart_add_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_user_cart_add_1d_sql}"
;;
"dws_trade_user_sku_order_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_user_sku_order_1d_sql}"
;;
"dws_trade_user_payment_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_trade_user_payment_1d_sql}"
;;
"dws_traffic_page_visitor_page_view_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_traffic_page_visitor_page_view_1d_sql}"
;;
"dws_traffic_session_page_view_1d")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; ${dws_traffic_session_page_view_1d_sql}"
;;
esac