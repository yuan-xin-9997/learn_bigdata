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

dws_trade_user_order_td="
insert overwrite table dws_trade_user_order_td partition (dt='$do_date')
select
       nvl(t1.user_id,t2.user_id) user_id,
       nvl(order_date_first,'$do_date') order_date_first,
       nvl(t2.dt,order_date_last) order_date_last,
       -- 数值类型： 左右都判null，之后相加
       nvl(order_count_td,0) + nvl(order_count_1d,0) order_count_td,
       nvl(order_num_td,0) + nvl(order_num_1d,0) order_num_td,
       nvl(original_amount_td,0) + nvl(order_original_amount_1d,0) original_amount_td,
       nvl(activity_reduce_amount_td,0) + nvl(activity_reduce_amount_1d,0) activity_reduce_amount_td,
       nvl(coupon_reduce_amount_td,0) + nvl(coupon_reduce_amount_1d,0) coupon_reduce_amount_td,
       nvl(total_amount_td,0) + nvl(order_total_amount_1d,0) total_amount_td
from
    (
        -- 截止到6-14日所有用户的下单统计
        select
            *
        from dws_trade_user_order_td
        where dt=date_sub('$do_date',1)

    ) t1
full join
    (
         -- 6-15日所有用户的下单统计
        select
            *
        from dws_trade_user_order_1d
        where dt='$do_date'

    )t2
on t1.user_id = t2.user_id;
"

dws_user_user_login_td="
insert overwrite table dws_user_user_login_td partition (dt='$do_date')
select
        nvl(t1.user_id,t2.user_id),
       nvl(t2.login_date,login_date_last) login_date_last,
       nvl(login_count_td,0) + nvl(login_count_1d,0) login_count_td
from
    (
        -- 截止到6-14日用户的登录情况
        select
            *
        from dws_user_user_login_td
        where dt=date_sub('$do_date',1)

    )t1
left join
    (
        -- 6-15日所有用户的登录情况?
        -- 肯定! 6-14日以后就开始记录用户行为，因此6-15日注册的用户的登录行为一会可以记录的
        select
            user_id,
           '$do_date' login_date,
            count(*) login_count_1d
        from dwd_user_login_inc
        where dt='$do_date'
        group by user_id

    )t2
on t1.user_id = t2.user_id;
"

case $1 in
"dws_trade_user_order_td")
	hive --database gmall -e "$dws_trade_user_order_td"
	;;
"dws_user_user_login_td")
	hive --database gmall -e "$dws_user_user_login_td"
	;;
"all")
	hive --database gmall -e "$dws_trade_user_order_td$dws_user_user_login_td"
	;;
esac

