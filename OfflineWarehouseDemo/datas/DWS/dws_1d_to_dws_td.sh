#! /bin/bash
#dws_1d_to_dws_td.sh all/表名 [日期]

# 1. 判断参数是否传入
if [ $# -lt 1 ];then
    echo "必须传表名/all"
    exit 1
fi

# 如果输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

dws_trade_user_order_td="
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
    from dws_trade_user_order_td where dt=date_sub('$do_date', 1)
), curr_agg as (
    select user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d,
           '$do_date' order_date_first,
           '$do_date' order_date_last
    from dws_trade_user_order_1d where dt='$do_date'
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
insert overwrite table dws_trade_user_order_td partition (dt='$do_date')
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
"

dws_user_user_login_td="
with before_agg as (
    select user_id,
           login_date_last,
           login_count_td
    from dws_user_user_login_td
    where dt=date_sub('$do_date', 1)
), curr_agg as (
    select
        user_id,
        dt,
        1 login_count -- 事实表一行数据代表登录一次
    from dwd_user_login_inc where dt='$do_date'
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
insert overwrite table dws_user_user_login_td partition (dt='$do_date')
select user_id,
       max(login_date_last),
       sum(login_count_td)
from full_user
group by user_id
;
"


# 2. 根据第一个参数匹配加载数据
case $1 in
"all")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict;${dws_trade_user_order_td};${dws_user_user_login_td};"
;;
"dws_trade_user_order_td")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict;${dws_trade_user_order_td};"
;;
"dws_user_user_login_td")
/opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict;${dws_user_user_login_td};"
;;
esac