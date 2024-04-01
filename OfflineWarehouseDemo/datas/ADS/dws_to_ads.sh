#!/bin/bash
# DWS层->ADS层 每日数据装载脚本
# dws_to_ads.sh all/表名 [日期]

# 1. 判断参数是否传入
if [ $# -lt 1 ]
then
    echo "必须至少传入一个表名/all..."
    exit
fi

# 2. 判断日期是否传入，如果传入日期则加载指定日期的数据，如果没有传入则加载前一天的数据
if [ "$2" == "" ];then
#     datestr`date -d '-1 day' +%F`
    datestr=$(date -d '-1 day' +%F)
else
    datestr=$2
fi
# 也可以换成这种写法
# [ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)
echo "当前要操作的日期是${datestr}"


# sql 语句
ads_coupon_stats="
insert overwrite table ads_coupon_stats
select * from ads_coupon_stats
union
select
    '${datestr}' dt,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from dws_tool_user_coupon_coupon_used_1d  -- 一个用户，使用一种coupon_id是一行
where dt='${datestr}'
group by coupon_id,coupon_name;
"

ads_new_order_user_stats="
insert overwrite table ads_new_order_user_stats
select *
from ads_new_order_user_stats
union
select '${datestr}' dt,
       recent_days,
       count(*)     new_order_user_count
from dws_trade_user_order_td
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '${datestr}'
  and order_date_first > date_sub('${datestr}', 30)
  and order_date_first > date_sub('${datestr}', recent_days)
group by recent_days
;
"

ads_order_by_province="

insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
-- 省份最近1日的的订单统计
select
    '${datestr}',
    1 recent_days,
    province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2 iso_code_3166_2,
       order_count_1d order_count,
       order_original_amount_1d order_total_amount
from dws_trade_province_order_1d  -- 1个省份是1行
where dt='${datestr}'
union all
-- 各省份最近7、30日的订单统计
select
    '${datestr}',
    recent_days,
    province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2 iso_code_3166_2,
       max(if(recent_days=7, order_count_7d, order_count_30d)) order_count,
       sum(if(recent_days=7, order_original_amount_7d, order_original_amount_30d)) order_total_amount
from dws_trade_province_order_nd  -- 1个省份是1行
lateral view explode(array(7,30)) tmp as recent_days
where dt='${datestr}'
group by recent_days, province_id  -- 下面这些字段，都可以由province_id推断出
         ,province_name,area_code,iso_code,iso_3166_2
;
"

ads_order_continuously_user_count="
insert overwrite table ads_order_continuously_user_count
select *
from ads_order_continuously_user_count
union
select '${datestr}' dt,
       7            recent_days,
       count(*)     order_continuously_user_count
from (select user_id
      from (select user_id,
                   dt, -- 下单日期
                   datediff(lead(dt, 2, '9999/12/31') over (partition by user_id order by dt), dt) diffnum
            from dws_trade_user_order_1d -- 表粒度：一行表示一位用户下单一次
            where dt > date_sub('${datestr}', 7)) t1
      where t1.diffnum = 2
      group by user_id) t2
;
"

ads_order_stats_by_cate="
insert overwrite table ads_order_stats_by_cate
select * from  ads_order_stats_by_cate
union
-- 求最近1天各品类的下单人数和下单数
select
    '${datestr}' dt,
    1 recent_days,
    category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
    sum(order_count_1d) order_count,
    count(distinct user_id) order_user_count
from dws_trade_user_sku_order_1d  -- 粒度：一个用户下单一个sku是一行
where dt='${datestr}'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name
    union all
-- 求最近7、30天各品类的下单人数和下单数
-- 如果一条记录的order_count_30d>0，这条记录的order_count_7d不一定也>0
select
    '${datestr}' dt,
    recent_days,
    category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
    sum(if(recent_days=7, order_count_7d, order_count_30d)) order_count,
    -- 需要判断此人在最近7天是否下单过
    --   特殊处理：如果一个品牌在最近7天没人买过，那么count(null)=null，需要在外层进行空值处理
    nvl(count(distinct if(recent_days=7 and order_count_7d=0, null, user_id)), 0) order_user_count  from dws_trade_user_sku_order_nd  -- 粒度：一个用户下单一个sku是一行
lateral view explode(array(7,30)) tmp as recent_days
where dt='${datestr}'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name, recent_days
    ;
"

ads_order_stats_by_tm="
insert overwrite table ads_order_stats_by_tm
select * from  ads_order_stats_by_tm
union
select
    '${datestr}' dt,
    1 recent_days,
    tm_id,
    tm_name,
    sum(order_count_1d) order_count,
    count(distinct user_id) order_user_count
from dws_trade_user_sku_order_1d  -- 粒度：一个用户下单一个sku是一行
where dt='${datestr}'
group by tm_id, tm_name
    union all
-- 求最近7、30天各品牌的下单人数和下单数
-- 如果一条记录的order_count_30d>0，这条记录的order_count_7d不一定也>0
select
    '${datestr}' dt,
    recent_days,
    tm_id,
    tm_name,
    sum(if(recent_days=7, order_count_7d, order_count_30d)) order_count,
    -- 需要判断此人在最近7天是否下单过
    --   特殊处理：如果一个品牌在最近7天没人买过，那么count(null)=null，需要在外层进行空值处理
    nvl(count(distinct if(recent_days=7 and order_count_7d=0, null, user_id)), 0) order_user_count  from dws_trade_user_sku_order_nd  -- 粒度：一个用户下单一个sku是一行
lateral view explode(array(7,30)) tmp as recent_days
where dt='${datestr}'
group by tm_id, tm_name, recent_days
    ;
"

ads_order_to_pay_interval_avg="
insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '${datestr}' dt,
    bigint(avg(to_unix_timestamp(payment_time) - to_unix_timestamp(order_time))) order_to_pay_interval_avg
from dwd_trade_trade_flow_acc  -- 分区：按照订单是否完成(确认收货)进行分区，未确认收货在9999-12-31分区，已确认收货的，按照收货日期分区
-- 取最近1日，所有已经支付的订单的数据
--        最近1日，所有支付的订单，可能确认收货了，在${datestr}分区
--        最近1日，所有支付的订单，未确认收货，在9999-12-31分区
where (dt='9999-12-31' or dt='${datestr}')
and isnotnull(payment_time) -- 只要支付的订单，isnotnull(a) 等价于 a is not null
;
"

ads_page_path="
insert overwrite table ads_page_path -- 为了避免小文件，所以使用覆盖，而不是insert into
select *
from ads_page_path
union
-- 使用union为了实现幂等性，union=union all + group by 全字段
select '${datestr}' dt,
       source,
       target,
       count(*)     path_count
from (select concat('step-', rn, ':', source_tmp)     source,
             concat('step-', rn + 1, ':', target_tmp) target
      from (select mid_id,
                   last_page_id,
                   page_id,
                   view_time,
                   session_id,
-- source: page_id
-- target: 当前行下一行的page_id
                   page_id                                                                    source_tmp,
                   lead(page_id, 1, 'None') over (partition by session_id order by view_time) target_tmp,
                   row_number() over (partition by session_id order by view_time)             rn
            from dwd_traffic_page_view_inc -- 一次页面访问是一行
            where dt = '${datestr}' -- 按照访问日期分区
           ) t1) t2
group by source, target;
"

ads_repeat_purchase_by_tm="
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '${datestr}' dt,
    30 recent_days,
    tm_id, tm_name,
   cast(sum(if(total_order_count_30d>1, 1, 0)) * 100/ count(*) as decimal(16, 2)) order_repeat_rate
from (
select
    user_id,
    tm_id,
    tm_name,
    sum(order_count_30d)  total_order_count_30d
from dws_trade_user_sku_order_nd  -- 粒度：一个人下单一种SKU是一行
where dt='${datestr}'
group by user_id,tm_id,tm_name  -- tm_name和tm_id是1对1的关系。统计出每个人下单每种品牌的次数，30天内
) t1
group by tm_id, tm_name
;
"

ads_sku_cart_num_top3_by_cate="
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select dt,
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
select
    '${datestr}' dt,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       -- 求这个sku，在其3级品类中的排名
        row_number() over (partition by  category3_id order by cart_num desc) rk
from (
select sku_id,
       sum(sku_num) cart_num
from dwd_trade_cart_full
where dt='${datestr}'
group by sku_id) t1
left join (
select id,
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
       create_time,
       dt
from dim_sku_full
where dt='${datestr}'
) t2
on t1.sku_id=t2.id
) t3
where rk <= 3;
"

ads_sku_favor_count_top3_by_tm="
insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select dt,
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_count,
       rk
from  (
select
    dt,
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_add_count_1d favor_count,
       row_number() over (partition by tm_id order by favor_add_count_1d desc) rk
from dws_interaction_sku_favor_add_1d
where dt='${datestr}'
) t1
where rk<=3
;
"

ads_traffic_stats_by_channel="
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel where dt!='${datestr}'
union
select
    '${datestr}',
    recent_days,
    channel,
    cast(case recent_days
        when 1 then uv_count_1d
        when 7 then uv_count_7d
        else uv_count_30d
    end as bigint) uv_count,
    case recent_days
        when 1 then avg_duration_sec_1d
        when 7 then avg_duration_sec_7d
        else avg_duration_sec_30d
    end avg_duration_sec,
    case recent_days
        when 1 then avg_page_count_1d
        when 7 then avg_page_count_7d
        else avg_page_count_30d
    end avg_page_count,
    case recent_days
        when 1 then sv_count_1d
        when 7 then sv_count_7d
        else sv_count_30d
    end sv_count,
    case recent_days
        when 1 then bound_rate_1d
        when 7 then bound_rate_7d
        else bound_rate_30d
    end bounce_rate
from(
select
    channel,
    count(distinct if(dt='${datestr}', mid_id, null)) uv_count_1d,
    avg(if(dt='${datestr}',during_time_1d,null))/1000 avg_duration_sec_1d,
    avg(if(dt='${datestr}',page_count_1d,null)) avg_page_count_1d,
    sum(if(dt='${datestr}' and page_count_1d=1, 1, 0)) sv_count_1d,
    sum(if(page_count_1d=1 and dt='${datestr}',1,0))/sum(if(dt='${datestr}' and page_count_1d=1, 1, 0)) bound_rate_1d,

    count(distinct if(dt>=date_sub('${datestr}',6), mid_id, null)) uv_count_7d,
    avg(if(dt>=date_sub('${datestr}',6),during_time_1d,null))/1000 avg_duration_sec_7d,
    avg(if(dt>=date_sub('${datestr}',6),page_count_1d,null)) avg_page_count_7d,
    sum(if(dt>=date_sub('${datestr}',6) and page_count_1d=1, 1, 0)) sv_count_7d,
    sum(if(page_count_1d=1 and dt>=date_sub('${datestr}',6),1,0))/sum(if(dt>=date_sub('${datestr}',6) and page_count_1d=1, 1, 0)) bound_rate_7d,

    count(distinct mid_id) uv_count_30d,
    avg(during_time_1d)/1000 avg_duration_sec_30d,
    avg(page_count_1d) avg_page_count_30d,
    count(1) sv_count_30d,
    sum(if(page_count_1d=1,1,0))/count(1) bound_rate_30d
from dws_traffic_session_page_view_1d
where dt<='${datestr}' and dt>=date_sub('${datestr}', 29)
group by channel) t1 lateral view explode(array(1,7,30)) tmp as recent_days;
"

ads_user_action="
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '202-06-14' dt,
    home_count, good_detail_count, cart_count, order_count, payment_count
from (
-- 计算首页浏览人数、浏览商品详情页人数
select
--     count(distinct user_id)
count(distinct if(page_id='home', user_id, null)) home_count,
count(distinct if(page_id='good_detail', user_id, null)) good_detail_count
from dwd_traffic_page_view_inc
where dt='${datestr}'
and user_id is not null  -- user_id 存在为null的
and (page_id='home' or page_id='good_detail')
-- group by page_id
) t1 join (
-- 计算加入购物车人数
select
    count(*) cart_count
from dws_trade_user_cart_add_1d  -- 存放的都是在当天发生了加购事实的人的统计，一行驶一个人
where dt='${datestr}'
)t2 join (
-- 计算下单人数
select
    count(*) order_count
from dws_trade_user_order_1d
where dt='${datestr}'
) t3 join (
-- 计算支付人数
select
    count(*) payment_count
from dws_trade_user_payment_1d
where dt='${datestr}'
) t4 ;
"

ads_user_change="
insert overwrite table ads_user_change
select * from  ads_user_change
union
select
    '${datestr}' dt,
user_churn_count,
user_back_count
from (
-- user_churn_count
         select count(user_id) user_churn_count
         from dws_user_user_login_td --粒度：一个用户是一行，粒度：每天都有一个分区，这个分区存放的是截止到这一天所有的用户
         where dt = '${datestr}'
           and datediff('${datestr}', login_date_last) = 7) t3
         join (
-- user_back_count
    select count(*) user_back_count
    from (
-- 今天登录用户
             select user_id
             from dws_user_user_login_td
             where dt = '${datestr}'
               and login_date_last = '${datestr}') t1
             join (

-- 截止到昨天最近连续7天及以上未活跃用户
        select user_id
        from dws_user_user_login_td
        where dt = date_sub('${datestr}', 1)
          and login_date_last <= date_sub('${datestr}', 8)) t2
                  on t1.user_id = t2.user_id) t4;
"

ads_user_retention="
insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '${datestr}'                                                        dt,
       create_date,
       datediff('${datestr}', create_date)                                 retention_day,
       count(t2.user_id)                                                   retention_count,
       count(t1.user_id)                                                   new_user_count,
       cast(count(t2.user_id) / count(t1.user_id) as decimal(16, 2)) * 100 retention_rate
from (
-- 计算6月7日-6月13日各注册的人
         select dt create_date,
                user_id
         from dwd_user_register_inc -- 一个用户注册一次是一行
         where dt between date_sub('${datestr}', 7) and date_sub('${datestr}', 1)) t1
         left join (
-- 计算${datestr}活跃登录的所有人
    select user_id
    from dwd_user_login_inc -- 粒度：一个用户的一次登录是一行
    where dt = '${datestr}'
    group by user_id) t2
                   on t1.user_id = t2.user_id
group by create_date
;
"

ads_user_stats="
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select
    '${datestr}' dt,
       nvl(t1.recent_days, t2.recent_days) recent_days,
       nvl(new_user_count, 0) new_user_count,
       nvl(active_user_count, 0) active_user_count
from (
-- 计算新增用户数
         select recent_days,
                count(*) new_user_count
         from dwd_user_register_inc -- 粒度：一行代表一个注册成功的用户
                  lateral view explode(array(1, 7, 30)) tmp as recent_days -- 第2步执行：使用侧窗函数列转行，复制3份数据，并添加recent_days
         where dt > date_sub('${datestr}', 30)          -- 第1步执行：取最近30天所有用户注册的数据
           and dt > date_sub('${datestr}', recent_days) -- 第3步：过滤出要统计的时间周期范围内的数据
         group by recent_days -- 将过滤后的复制了3份的数据集按照recent_days分组
     ) t1
         full join -- 此处应该使用full join！！！两个数据没有业务上的联系，最近范围的注册和登录没有联系
    (
-- 计算活跃用户数
        select recent_days,
               count(*) active_user_count
        from dws_user_user_login_td -- 粒度：在每个分区中，一个用户是一行
                 lateral view explode(array(1, 7, 30)) tmp as recent_days
        where dt = '${datestr}'
          and login_date_last > date_sub('${datestr}', 30)          -- 只统计最近1,7,30天登录过的用户，把30天内登录的用户过滤出来
          and login_date_last > date_sub('${datestr}', recent_days) -- 过滤此每一份中，在所有统计日期内登录的用户
        group by recent_days) t2
on t1.recent_days=t2.recent_days
;
"








case $1 in
"ads_coupon_stats")
  /opt/module/hive/bin/hive --database gmall -e "$ads_coupon_stats"
  ;;
"ads_new_order_user_stats")
  /opt/module/hive/bin/hive --database gmall -e "$ads_new_order_user_stats"
  ;;
"ads_order_by_province")
  /opt/module/hive/bin/hive --database gmall -e "$ads_order_by_province"
  ;;
"ads_order_continuously_user_count")
  /opt/module/hive/bin/hive --database gmall -e "$ads_order_continuously_user_count"
  ;;
"ads_order_stats_by_cate")
  /opt/module/hive/bin/hive --database gmall -e "$ads_order_stats_by_cate"
  ;;
"ads_order_stats_by_tm")
  /opt/module/hive/bin/hive --database gmall -e "$ads_order_stats_by_tm"
  ;;
"ads_order_to_pay_interval_avg")
  /opt/module/hive/bin/hive --database gmall -e "$ads_order_to_pay_interval_avg"
  ;;
"ads_page_path")
  /opt/module/hive/bin/hive --database gmall -e "$ads_page_path"
  ;;
"ads_repeat_purchase_by_tm")
  /opt/module/hive/bin/hive --database gmall -e "$ads_repeat_purchase_by_tm"
  ;;
"ads_sku_cart_num_top3_by_cate")
  /opt/module/hive/bin/hive --database gmall -e "$ads_sku_cart_num_top3_by_cate"
  ;;
"ads_sku_favor_count_top3_by_tm")
  /opt/module/hive/bin/hive --database gmall -e "$ads_sku_favor_count_top3_by_tm"
  ;;
"ads_traffic_stats_by_channel")
  /opt/module/hive/bin/hive --database gmall -e "$ads_traffic_stats_by_channel"
  ;;
"ads_user_action")
  /opt/module/hive/bin/hive --database gmall -e "$ads_user_action"
  ;;
"ads_user_change")
  /opt/module/hive/bin/hive --database gmall -e "$ads_user_change"
  ;;
"ads_user_retention")
  /opt/module/hive/bin/hive --database gmall -e "$ads_user_retention"
  ;;
"ads_user_stats")
  /opt/module/hive/bin/hive --database gmall -e "$ads_user_stats"
  ;;
"all")
  /opt/module/hive/bin/hive --database gmall -e "${ads_coupon_stats};${ads_new_order_user_stats};${ads_order_by_province};${ads_order_continuously_user_count};${ads_order_stats_by_cate};${ads_order_stats_by_tm};${ads_order_to_pay_interval_avg};${ads_page_path};${ads_repeat_purchase_by_tm};${ads_sku_cart_num_top3_by_cate};${ads_sku_favor_count_top3_by_tm};${ads_traffic_stats_by_channel};${ads_user_action};${ads_user_change};${ads_user_retention};${ads_user_stats};"
  ;;
esac