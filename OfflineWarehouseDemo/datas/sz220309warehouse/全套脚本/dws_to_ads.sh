#!/bin/bash
#-n代表判断字符串的长度，如果字符串长度为0，返回false,否则返回true
if [ -n "$2" ]
then
	do_date=$2
else
	#echo 必须给我传一个日期
	#exit
	do_date= $(date -d yesterday +%F) 
fi

echo 当前要操作的日期是$do_date

ads_coupon_stats="
   insert overwrite table ads_coupon_stats
     select * from ads_coupon_stats
     union 
select
      '$do_date' dt,
       coupon_id,
       coupon_name,
       sum(used_count_1d) used_count,
       count(*) used_user_count
-- 一个用户，使用的一种coupon_id是一行
from dws_tool_user_coupon_coupon_used_1d
where dt='$do_date'
group by coupon_id,coupon_name;
"
ads_new_order_user_stats="
insert overwrite table ads_new_order_user_stats
     select * from ads_new_order_user_stats
     union 
select
       '$do_date' dt,
    recent_days,
       count(*) new_order_user_count
from dws_trade_user_order_td
lateral view explode(\`array\`(1, 7, 30)) tmp as recent_days
where dt= '$do_date'
-- 过滤得到在最近30天第一次下单的用户
and order_date_first > date_sub('$do_date',30)
-- 过滤得到对应recent_days统计周期的用户
and order_date_first > date_sub('$do_date',recent_days)
group by recent_days;
"
ads_order_by_province="
 insert overwrite table ads_order_by_province
     select * from ads_order_by_province
     union 
-- 各省份最近1日的
select
      '$do_date',
         1 recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2 iso_code_3166_2,
       order_count_1d  order_count,
       order_total_amount_1d order_total_amount
-- 1个省份是一行
from dws_trade_province_order_1d
where dt='$do_date'
union all
-- 各省份最近7，30日的
select
       '$do_date',
          recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2 iso_code_3166_2,
       max(\`if\`(recent_days = 7 ,order_count_7d ,order_count_30d))  order_count,
       sum(\`if\`(recent_days = 7 ,order_total_amount_7d ,order_total_amount_30d))  order_total_amount
-- 1个省份是一行
from dws_trade_province_order_nd
lateral view explode(\`array\`( 7, 30)) tmp as recent_days
where dt='$do_date'
group by recent_days,province_id,
         -- 这些字段和 province_id 是1：1关系
          province_name,
       area_code,
       iso_code,
       iso_3166_2;

"
ads_order_continuously_user_count="
insert overwrite table ads_order_continuously_user_count
     select * from ads_order_continuously_user_count
     union 
select
    '$do_date' dt,
       7 ,
    count(*) order_continuously_user_count
from (
         select user_id
         from (
                  select user_id,
                         -- 下单日期
                         dt,
                         datediff(lead(dt, 2, '9999/12/31') over (partition by user_id order by dt ), dt) diffnum
                  from dws_trade_user_order_1d
                  where dt > date_sub('$do_date', 7)
              ) t1
         where diffnum = 2
         group by user_id
     ) t2;
"
ads_order_to_pay_interval_avg="
insert overwrite table ads_order_to_pay_interval_avg
      select * from ads_order_to_pay_interval_avg
      union 
select
        '$do_date',
        bigint(avg(to_unix_timestamp(payment_time) -  to_unix_timestamp(order_time)))
-- 分区：按照订单是否完成(确认收货)进行分区，未确认收货的在9999-12-31分区，已经确认收货的，按照收货日期分区
from dwd_trade_trade_flow_acc
-- 取最近1日，所有已经支付的订单的数据。
-- 最近1日，所有支付的订单，可能确认收货了，在 $do_date分区
-- 最近1日，所有支付的订单，未确认收货，在 9999-12-31分区
where (dt = '9999-12-31' or dt='$do_date')
-- 只要支付的订单
  -- isnotnull(a) 等价于 a is not null
and isnotnull(payment_time);
"
ads_page_path="
insert overwrite table ads_page_path
     select * from ads_page_path
 -- 实现幂等性  union 是先union all 再 group by 全字段
     union
select
       '$do_date' dt,
        source,target,
       count(*) path_count
from (
         select concat('step-', rn, ':', source)     source,
                concat('step-', rn + 1, ':',target) target
         from (
                  select
                      -- source: page_id
                      -- target : 当前行下一行的page_id
                      mid_id,
                      last_page_id,
                      page_id,
                      view_time,
                      session_id,
                      page_id                                                                    source,
                      lead(page_id, 1, 'NONE') over (partition by session_id order by view_time) target,
                      row_number() over (partition by session_id order by view_time)             rn
-- 一次页面访问是一行
                  from dwd_traffic_page_view_inc
-- 按照访问日期分区
                  where dt = '$do_date'
              ) t1
     ) t2
group by source,target;
"
ads_repeat_purchase_by_tm="
insert overwrite table ads_repeat_purchase_by_tm
     select * from ads_repeat_purchase_by_tm
     union 
select
       '$do_date',
        30,
        tm_id,tm_name,
        cast(sum(\`if\`(total_order_count_30d > 1,1,0)) / count(*) * 100 as decimal(16,2)) order_repeat_rate
from (
         select user_id,
                tm_id,
                tm_name,
                sum(order_count_30d) total_order_count_30d
-- 一个人下单一种sku是一行
         from dws_trade_user_sku_order_nd
         where dt = '$do_date'
-- 先统计出每个人下单每种品牌的次数
         group by user_id, tm_id
                -- tm_name 和 tm_id是1对1的关系
                , tm_name
     ) t1
group by tm_id,tm_name;
"
ads_sku_cart_num_top3_by_cate="
insert overwrite table ads_sku_cart_num_top3_by_cate
     select * from ads_sku_cart_num_top3_by_cate
     union 
select
   '$do_date' dt,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       rk
from (
         select category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sku_id,
                sku_name,
                cart_num,
                -- 求这个sku 在其 3级品类中的排名
                row_number() over (partition by category3_id order by cart_num desc) rk
         from (--求最近1日，所有用户购物车中存量的商品前3
                  select sku_id,
                         sum(sku_num) cart_num
-- 一个user，购物车中的一个sku是一行
                  from dwd_trade_cart_full
                  where dt = '$do_date'
                  group by sku_id
              ) t1
                  left join
              (
                  select id,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name,
                         sku_name
                  from dim_sku_full
                  where dt = '$do_date'
              ) t2
              on t1.sku_id = t2.id
     ) t3
where rk <= 3;
"
ads_sku_favor_count_top3_by_tm="
insert overwrite table ads_sku_favor_count_top3_by_tm
     select * from ads_sku_favor_count_top3_by_tm
     union 
select
     '$do_date' dt,
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_count,
       rk
from (
         select dt,
                tm_id,
                tm_name,
                sku_id,
                sku_name,
                favor_add_count_1d                                                       favor_count,
                row_number() over (partition by tm_id order by favor_add_count_1d desc ) rk

         from dws_interaction_sku_favor_add_1d
         where dt = '$do_date'
     ) t1
where  rk <= 3;
"
ads_trade_stats_by_cate="
insert overwrite table ads_trade_stats_by_cate
     select * from ads_trade_stats_by_cate
     union
-- 求最近1天各品类的下单人数和下单数
select
       '$do_date',
      1 recent_days,
     category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
       sum(order_count_1d) order_count,
       count(distinct user_id) order_user_count
from dws_trade_user_sku_order_1d
where dt='$do_date'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name
            -- 和category3_id 都是 1:1的关系，直接加在group by 后面

union all
select
        '$do_date',recent_days,
    category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
       sum(\`if\`(recent_days = 7 , order_count_7d,order_count_30d)) order_count,
       -- 先判断这个人是否买过
       -- 特殊处理： 如果一个品牌在最近7天没人买过，那么count(null) = null，进行空值处理
       nvl(count(distinct  \`if\`(recent_days = 7 and order_count_7d = 0,null,user_id)),0)  order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_nd
lateral view explode(\`array\`( 7, 30)) tmp as recent_days
where dt='$do_date'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,recent_days;

"
ads_trade_stats_by_tm="
insert overwrite table ads_trade_stats_by_tm
     select * from ads_trade_stats_by_tm
     union
-- 求最近1天各品牌的下单人数和下单数
select
       '$do_date',
      1 recent_days,
    tm_id,tm_name,
       sum(order_count_1d) order_count,
       count(distinct user_id) order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_1d
where dt='$do_date'
group by tm_id,tm_name
union all
-- 求最近7,30天各品牌的下单人数和下单数
-- 如果一条记录的 order_count_30d > 0 ,这条记录的 order_count_7d 是否一定也 > 0 ?
--  jack最近30天下单了 iphone13，是否最近7天也下单了 iphone13? 不一定。 每一行数据的 order_count_7d，有可能为0
select
        '$do_date',recent_days,
    tm_id,tm_name,
       sum(\`if\`(recent_days = 7 , order_count_7d,order_count_30d)) order_count,
       -- 先判断这个人是否买过
       -- 特殊处理： 如果一个品牌在最近7天没人买过，那么count(null) = null，进行空值处理
       nvl(count(distinct  \`if\`(recent_days = 7 and order_count_7d = 0,null,user_id)),0)  order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_nd
lateral view explode(\`array\`( 7, 30)) tmp as recent_days
where dt='$do_date'
group by tm_id,tm_name,recent_days;
"
ads_traffic_stats_by_channel="
  insert overwrite table ads_traffic_stats_by_channel
    select * from ads_traffic_stats_by_channel
  -- union 拼接后去重 union= union all + group by 全字段
    union
    select
       '$do_date' dt,
        recent_days,
        channel,
        count(distinct mid_id) uv_count,
        bigint(avg(during_time_1d)) avg_duration_sec,
        bigint(avg(page_count_1d)) avg_page_count,
        count(*) sv_count,
        cast(sum(\`if\`(page_count_1d = 1,1,0))  / count(*) * 100 as  decimal(16, 2)) bounce_rate
    -- 粒度是一个session是一行
    from dws_traffic_session_page_view_1d
    lateral view explode(\`array\`(1, 7, 30)) tmp as recent_days
     -- 取最近30天的数据
    where dt > date_sub('$do_date',30)
        and dt > date_sub('$do_date',recent_days)
    group by channel,recent_days;

"
ads_user_action="
insert overwrite table ads_user_action
     select * from ads_user_action
     union 
select
      '$do_date' dt,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (
         select count(distinct \`if\`(page_id = 'home', user_id, null))        home_count,       -- 315
                count(distinct \`if\`(page_id = 'good_detail', user_id, null)) good_detail_count --267
-- user_id存在为null
         from dwd_traffic_page_view_inc
         where dt = '$do_date'
           and user_id is not null
           and (page_id = 'home' or page_id = 'good_detail')
     ) t1
join

     (
         select count(*) cart_count
-- 存放的都是在当天发生了加购事实的人的统计，一行是一个人
         from dws_trade_user_cart_add_1d
         where dt = '$do_date'
     ) t2
join
     (
         select count(*) order_count
         from dws_trade_user_order_1d
         where dt = '$do_date'
     ) t3
join
     (
         select count(*) payment_count
         from dws_trade_user_payment_1d
         where dt = '$do_date'
     ) t4;
"
ads_user_change="
 insert overwrite table ads_user_change
      select * from ads_user_change
      union 
select
   '$do_date' dt,
        user_churn_count,
        user_back_count

from (-- user_churn_count : 截止到今天，所有用户中最后一次登录日期距离今天刚好7天的用户

         select count(user_id) user_churn_count
-- 粒度： 一个用户是一行
         from dws_user_user_login_td
-- 分区： 每天都有一个分区，这个分区存放的是截止到这一天所有的用户
         where dt = '$do_date'
           and datediff('$do_date', login_date_last) = 7
     ) t3
join
     (-- user_back_count: 今天登录且截止到昨天最近连续7天及以上未活跃用户
         select count(*) user_back_count
         from (-- 今天登录的用户
                  select user_id
                  from dws_user_user_login_td
                  where dt = '$do_date'
                    and login_date_last = '$do_date'
              ) t1
                  join
              (-- 截止到昨天最近连续7天及以上未活跃的用户
                  select user_id
                  from dws_user_user_login_td
                  where dt = date_sub('$do_date', 1)
                    and login_date_last <= date_sub('$do_date', 8)
              ) t2
              on t1.user_id = t2.user_id
     ) t4;
"
ads_user_retention="
insert overwrite table ads_user_retention
     select * from ads_user_retention
     union 
select
        '$do_date' dt,
        create_date,
        datediff('$do_date',create_date) retention_day,
        count(t2.user_id) retention_count,
        count(t1.user_id) new_user_count,
        cast(count(t2.user_id) / count(t1.user_id) * 100 as decimal(16, 2)) retention_rate
from (-- 求 6-07 ---- 6-13日各注册的人
         select dt create_date,
                user_id
-- 一个用户是一行
         from dwd_user_register_inc
         where dt between date_sub('$do_date', 7) and date_sub('$do_date', 1)
     ) t1
left join

     (-- 求6-14日活跃(登录)的所有人
         select user_id
-- 粒度： 一个用户的一次登录是一行
         from dwd_user_login_inc
         where dt = '$do_date'
         group by user_id
     ) t2
on t1.user_id = t2.user_id
group by create_date;
"
ads_user_stats="
insert overwrite table ads_user_stats
     select * from ads_user_stats
     union
select
       '$do_date' dt,
        nvl(t1.recent_days,t2.recent_days) recent_days,
       nvl(new_user_count,0) new_user_count,
       nvl(active_user_count,0) active_user_count
from (
         select recent_days,
                count(*) new_user_count
-- 粒度： 一个用户是一行
         from dwd_user_register_inc
                  -- ②复制3份
                  lateral view explode(\`array\`(1, 7, 30)) tmp as recent_days
-- ①取最近30天所有用户注册的数据
         where dt > date_sub('$do_date', 30)
-- ③过滤出要统计的时间周期范围内的数据
           and dt > date_sub('$do_date', recent_days)
-- ④将过滤后的复制了3份的数据集按照 recent_days分组
         group by recent_days
     ) t1
full join
     (
         select recent_days,
                count(*) active_user_count
-- 粒度： 在每个分区中，一个用户是一行
         from dws_user_user_login_td
                  lateral view explode(\`array\`(1, 7, 30)) tmp as recent_days
         where dt = '$do_date'
-- 只统计最近1,7,30天登录过的用户
-- 把30天内登录过的用户过滤出来
           and login_date_last > date_sub('$do_date', 30)
-- 过滤此，每一份中，在所统计的日期范围内登录的用户
           and login_date_last > date_sub('$do_date', recent_days)
         group by recent_days
     ) t2
on t1.recent_days = t2.recent_days;
"


case $1 in
"ads_coupon_stats")
   hive --database gmall -e "$ads_coupon_stats"
   ;;
"ads_new_order_user_stats")
   hive --database gmall -e "$ads_new_order_user_stats"
   ;;
"ads_order_by_province")
   hive --database gmall -e "$ads_order_by_province"
   ;;
"ads_order_continuously_user_count")
   hive --database gmall -e "$ads_order_continuously_user_count"
   ;;
"ads_order_to_pay_interval_avg")
   hive --database gmall -e "$ads_order_to_pay_interval_avg"
   ;;
"ads_page_path")
   hive --database gmall -e "$ads_page_path"
   ;;
"ads_repeat_purchase_by_tm")
   hive --database gmall -e "$ads_repeat_purchase_by_tm"
   ;;
"ads_sku_cart_num_top3_by_cate")
   hive --database gmall -e "$ads_sku_cart_num_top3_by_cate"
   ;;
"ads_sku_favor_count_top3_by_tm")
   hive --database gmall -e "$ads_sku_favor_count_top3_by_tm"
   ;;
"ads_trade_stats_by_cate")
   hive --database gmall -e "$ads_trade_stats_by_cate"
   ;;
"ads_trade_stats_by_tm")
   hive --database gmall -e "$ads_trade_stats_by_tm"
   ;;
"ads_traffic_stats_by_channel")
   hive --database gmall -e "$ads_traffic_stats_by_channel"
   ;;
"ads_user_action")
   hive --database gmall -e "$ads_user_action"
   ;;
"ads_user_change")
   hive --database gmall -e "$ads_user_change"
   ;;
"ads_user_retention")
   hive --database gmall -e "$ads_user_retention"
   ;;
"ads_user_stats")
   hive --database gmall -e "$ads_user_stats"
   ;;
"all")
   hive --database gmall -e "$ads_coupon_stats$ads_new_order_user_stats$ads_order_by_province$ads_order_continuously_user_count$ads_order_to_pay_interval_avg$ads_page_path$ads_repeat_purchase_by_tm$ads_sku_cart_num_top3_by_cate$ads_sku_favor_count_top3_by_tm$ads_trade_stats_by_cate$ads_trade_stats_by_tm$ads_traffic_stats_by_channel$ads_user_action$ads_user_change$ads_user_retention$ads_user_stats"
   ;;
esac
