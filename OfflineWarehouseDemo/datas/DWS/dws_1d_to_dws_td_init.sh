#! /bin/bash
# dws_1d_to_dws_td_init.sh all/表名
# 首日已经确定，不要传

# 1. 判断参数是否传入
if [ $# -lt 1 ];then
    echo "必须传表名/all"
    exit 1
fi

dws_trade_user_order_td="
insert overwrite table dws_trade_user_order_td partition (dt='2020-06-14')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from dws_trade_user_order_1d
where dt<='2020-06-14'
group by user_id
;
"

dws_user_user_login_td="
with full_user as (select id,
                          date_format(create_time, 'yyyy-MM-dd') date_id
                   from dim_user_zip
                   where dt = '9999-12-31'
                     and date_format(create_time, 'yyyy-MM-dd') <= '2020-06-14'), login_user as (
select
    user_id,
    date_id
from dwd_user_login_inc where dt='2020-06-14'
), full_login_user as (
    select id,
           date_id
    from full_user
    union all
    select user_id, date_id
    from login_user
)
insert overwrite table dws_user_user_login_td partition (dt='2020-06-14')
select
    id,
    -- min(date_id) login_date_first,
    max(date_id) login_date_last,
    count(1) login_count_td
from full_login_user
group by id
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
