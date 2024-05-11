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
    user_id,
       -- 首次下单日期
        min(dt) order_date_first,
       -- 末次下单日期
        max(dt) order_date_last,
        sum(order_count_1d) order_count_td,
        sum(order_num_1d) order_num_td,
        sum(order_original_amount_1d ) original_amount_td,
        sum(activity_reduce_amount_1d) activity_reduce_amount_td,
        sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
        sum(order_total_amount_1d) total_amount_td
from dws_trade_user_order_1d
where dt <= '$do_date'
group by user_id;
"

dws_user_user_login_td="
 -- 要统计的是截止到6-14日，全部用户及其登录的情况
insert overwrite table dws_user_user_login_td partition (dt='$do_date')
select
        t1.id user_id,
       -- 末次登录日期
        nvl(login_date,regist_date) login_date_last,
        nvl(login_count_1d,1) login_count_td

from (
         select id,
                date_format(create_time, 'yyyy-MM-dd') regist_date
         from dim_user_zip
              -- 存放的是所有用户的最新信息
         where dt = '9999-12-31'
     ) t1
left join
     (-- 仅仅在6-14日当天登录的用户(不是全部用户)
         select user_id,
                '$do_date' login_date,
                count(*)     login_count_1d
                --粒度： 一个用户的一次登录是一行
         from dwd_user_login_inc
         where dt = '$do_date'
         group by user_id
     ) t2
on t1.id = t2.user_id;
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