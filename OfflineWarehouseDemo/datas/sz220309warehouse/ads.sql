
show databases ;

/*
        套路：
                    ①分析当啊表数据的导入源， 输入
                    ②当前表要求的输出  粒度，行数
                    ③结合 ①②推理计算过程
                                a) 目标表取数据，在取数据之前，搞清楚目标表的粒度
                                b) 搞清楚目标表如何分区

        ads_page_path和业务数据无关。和log数据有关。

        优先去dws层(汇总层)，如果dws建模不够完善，会导致ADS需求找不到对应的DWS表，只能回溯DWD


 */

-- 为了避免小文件
 insert overwrite table ads_page_path
     select * from ads_page_path
 -- 实现幂等性  union 是先union all 再 group by 全字段
     union
select
       '2020-06-14' dt,
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
                  where dt = '2020-06-14'
              ) t1
     ) t2
group by source,target;


/*
        问题一： 为什么不用分区统计？
                        ads层的表都不是分区表

        问题二：  为什么ads不建为分区表？

        问题三： 为什么你的ods,dim,dwd,dws要建分区表？

        问题四：  ads_page_path 为什么不统计之前的数据。
                    如果ads_xxxx的字段中有 周期，需求中指定了周期，按照周期的范围统计。

                    没有指定周期，都是统计最新1天的数据。


 */


 select dt,
        user_churn_count,
        user_back_count
 from ads_user_change;

/*
        表格1：  m * a
                m是主键，唯一的，没有重复的
        表格2:   n * b
                 n是主键，唯一的，没有重复的

        表格1 笛卡尔积 表格2 结果：  m * n 行    *   （a+b）列

 */
  insert overwrite table ads_user_change
      select * from ads_user_change
      union 
select
   '2020-06-14' dt,
        user_churn_count,
        user_back_count

from (-- user_churn_count : 截止到今天，所有用户中最后一次登录日期距离今天刚好7天的用户

         select count(user_id) user_churn_count
-- 粒度： 一个用户是一行
         from dws_user_user_login_td
-- 分区： 每天都有一个分区，这个分区存放的是截止到这一天所有的用户
         where dt = '2020-06-14'
           and datediff('2020-06-14', login_date_last) = 7
     ) t3
join
     (-- user_back_count: 今天登录且截止到昨天最近连续7天及以上未活跃用户
         select count(*) user_back_count
         from (-- 今天登录的用户
                  select user_id
                  from dws_user_user_login_td
                  where dt = '2020-06-14'
                    and login_date_last = '2020-06-14'
              ) t1
                  join
              (-- 截止到昨天最近连续7天及以上未活跃的用户
                  select user_id
                  from dws_user_user_login_td
                  where dt = date_sub('2020-06-14', 1)
                    and login_date_last <= date_sub('2020-06-14', 8)
              ) t2
              on t1.user_id = t2.user_id
     ) t4;


/*
    ads_user_retention

    留存天数n(retention_day) =  当前活跃日期(dt) - 用户新增日期(create_date)

    有6-14日的登录数据，可以求 6-13日注册的那批人的 留存了1日的 留存率
    有6-14日的登录数据，可以求 6-12日注册的那批人的 留存了2日的 留存率
    有6-14日的登录数据，可以求 6-11日注册的那批人的 留存了3日的 留存率
    有6-14日的登录数据，可以求 6-10日注册的那批人的 留存了4日的 留存率
    有6-14日的登录数据，可以求 6-09日注册的那批人的 留存了5日的 留存率
    有6-14日的登录数据，可以求 6-08日注册的那批人的 留存了6日的 留存率
    有6-14日的登录数据，可以求 6-07日注册的那批人的 留存了7日的 留存率
 */

 select dt,
        create_date,
        retention_day,
        retention_count,
        new_user_count,
        retention_rate
 from ads_user_retention;

 insert overwrite table ads_user_retention
     select * from ads_user_retention
     union 
select
        '2020-06-14' dt,
        create_date,
        datediff('2020-06-14',create_date) retention_day,
        count(t2.user_id) retention_count,
        count(t1.user_id) new_user_count,
        cast(count(t2.user_id) / count(t1.user_id) * 100 as decimal(16, 2)) retention_rate
from (-- 求 6-07 ---- 6-13日各注册的人
         select dt create_date,
                user_id
-- 一个用户是一行
         from dwd_user_register_inc
         where dt between date_sub('2020-06-14', 7) and date_sub('2020-06-14', 1)
     ) t1
left join

     (-- 求6-14日活跃(登录)的所有人
         select user_id
-- 粒度： 一个用户的一次登录是一行
         from dwd_user_login_inc
         where dt = '2020-06-14'
         group by user_id
     ) t2
on t1.user_id = t2.user_id
group by create_date;


/*
        join的方式？
                left join? inner join? full join?

        full join!

        最近1天，假设没有人注册！

        这个数据没有业务上的联系。 最近范围的注册和登录没有联系，取全部。
 */



select dt,
       recent_days,
       new_user_count,
       active_user_count
from ads_user_stats;


 insert overwrite table ads_user_stats
     select * from ads_user_stats
     union
select
       '2020-06-14' dt,
        nvl(t1.recent_days,t2.recent_days) recent_days,
       nvl(new_user_count,0) new_user_count,
       nvl(active_user_count,0) active_user_count
from (
         select recent_days,
                count(*) new_user_count
-- 粒度： 一个用户是一行
         from dwd_user_register_inc
                  -- ②复制3份
                  lateral view explode(`array`(1, 7, 30)) tmp as recent_days
-- ①取最近30天所有用户注册的数据
         where dt > date_sub('2020-06-14', 30)
-- ③过滤出要统计的时间周期范围内的数据
           and dt > date_sub('2020-06-14', recent_days)
-- ④将过滤后的复制了3份的数据集按照 recent_days分组
         group by recent_days
     ) t1
full join
     (
         select recent_days,
                count(*) active_user_count
-- 粒度： 在每个分区中，一个用户是一行
         from dws_user_user_login_td
                  lateral view explode(`array`(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
-- 只统计最近1,7,30天登录过的用户
-- 把30天内登录过的用户过滤出来
           and login_date_last > date_sub('2020-06-14', 30)
-- 过滤此，每一份中，在所统计的日期范围内登录的用户
           and login_date_last > date_sub('2020-06-14', recent_days)
         group by recent_days
     ) t2
on t1.recent_days = t2.recent_days;



select dt,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from ads_user_action;

 insert overwrite table ads_user_action
     select * from ads_user_action
     union 
select
      '2020-06-14' dt,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (
         select count(distinct `if`(page_id = 'home', user_id, null))        home_count,       -- 315
                count(distinct `if`(page_id = 'good_detail', user_id, null)) good_detail_count --267
-- user_id存在为null
         from dwd_traffic_page_view_inc
         where dt = '2020-06-14'
           and user_id is not null
           and (page_id = 'home' or page_id = 'good_detail')
     ) t1
join

     (
         select count(*) cart_count
-- 存放的都是在当天发生了加购事实的人的统计，一行是一个人
         from dws_trade_user_cart_add_1d
         where dt = '2020-06-14'
     ) t2
join
     (
         select count(*) order_count
         from dws_trade_user_order_1d
         where dt = '2020-06-14'
     ) t3
join
     (
         select count(*) payment_count
         from dws_trade_user_payment_1d
         where dt = '2020-06-14'
     ) t4;




select dt,
       recent_days,
       new_order_user_count
from ads_new_order_user_stats;

 insert overwrite table ads_new_order_user_stats
     select * from ads_new_order_user_stats
     union 
select
       '2020-06-14' dt,
    recent_days,
       count(*) new_order_user_count
from dws_trade_user_order_td
lateral view explode(`array`(1, 7, 30)) tmp as recent_days
where dt= '2020-06-14'
-- 过滤得到在最近30天第一次下单的用户
and order_date_first > date_sub('2020-06-14',30)
-- 过滤得到对应recent_days统计周期的用户
and order_date_first > date_sub('2020-06-14',recent_days)
group by recent_days;


/*
    ads_order_continuously_user_count

    套路： 把我所求的符合连续特征的数据的规律，按照规律进行统计


    连续3日下单
            统计时，样本范围至少3行	在3行的范围内，如果第一行和最后一行，相差2，他们就是连续的

74
523
22

 */

 select dt,
        recent_days,
        order_continuously_user_count
 from ads_order_continuously_user_count;

select
    '2020-06-15' dt,
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
                  where dt > date_sub('2020-06-15', 7)
              ) t1
         where diffnum = 2
         group by user_id
     ) t2;


/*
        把我连续： 使用参照物，判断是否连续

        假设有两列 A,B, A列是连续的，每次递增X，B列也是连续的，每次递增Y。
        那么这两列的差值列， 连续两行的差值是固定的都是 Y-X
        A       B       B和A的差值
        a       b       b-a
        a+X     b+Y     (b-a) + (Y-X)
        a+2X    b+2Y     (b-a) + 2(Y-X)
        a+3X    b+3Y


        A列： 日期， 增量X为1
        只需要提供一个也是连续的参照列，只要这一列和日期列的差值的列的连续两行的差值是固定的，那么就可以证明日期是连续的！

        dt                参照列(增量和日期列一致) 行号         差值列
        2020-06-10              1                          2020-06-09
        2020-06-11              2                          2020-06-09
        2020-06-12              3                           2020-06-09
        2020-06-15              4                           2020-06-11
        2020-06-17              5                           2020-06-12
        2020-06-19              6                           2020-06-13

 */

select
         '2020-06-15' dt,
       7 ,
        count(distinct user_id)
from (
         select user_id
         from (
                  select user_id,
                         -- 下单日期
                         dt,
                         row_number() over (partition by user_id order by dt )               rn,
                         date_sub(dt, row_number() over (partition by user_id order by dt )) diff
                  from dws_trade_user_order_1d
                  where dt > date_sub('2020-06-15', 7)
              ) t1
         group by user_id, diff
-- 连续3天  连续n天，需要  count(*) >= n
         having count(*) >= 3
     ) t2;


/*
        将一个人在最近7天所有的下单日期升序排序后，补齐日期。如果这个人在每一天下单了就1，否则记0

        形成： user_id ,  1001001

 */

select
      '2020-06-15' dt,
       7 ,
        count(user_id)
from (
         select user_id,
                sum(flag) sum_flag
         from (
                  select user_id,
                         -- 下单日期
                         dt,
                         case dt
                             when date_sub('2020-06-15', 0) then 1
                             when date_sub('2020-06-15', 1) then 10
                             when date_sub('2020-06-15', 2) then 100
                             when date_sub('2020-06-15', 3) then 1000
                             when date_sub('2020-06-15', 4) then 10000
                             when date_sub('2020-06-15', 5) then 100000
                             when date_sub('2020-06-15', 6) then 1000000
                             else 0
                             end flag
                  from dws_trade_user_order_1d
                  where dt > date_sub('2020-06-15', 7)
              ) t1
         group by user_id
     ) t2
where sum_flag like  '%111%';

select
    user_id,finalflag
from (
         select user_id,
                concat_ws('', collect_list(flag)) finalflag
         from (select user_id, -- 下单日期
                      dt,
                      if(datediff(dt, lag(dt, 1, dt) over (partition by user_id order by dt)) < 2, '1', '0') flag

               from dws_trade_user_order_1d
               where dt > date_sub('2020-06-15', 7)
              ) t1
         group by user_id
     ) t2
where finalflag like '%111%';



/*
 如果断一天也算连续（例如2020-06-12,2020-06-13,2020-06-15），应该如何计算？

 连续3日下单，何为断一天也算连续：
        第一种：   连续3天中，只允许断一天
        第二种：   断一天也算连续  2020-06-12,2020-06-14,2020-06-16


    统计时，样本范围至少3行	在3行的范围内，如果第一行和最后一行，相差n，他们就是连续的
 */
 -- 第一种：   连续3天中，只允许断一天
select
    '2020-06-15' dt,
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
                  where dt > date_sub('2020-06-15', 7)
              ) t1
         where diffnum < 4
         group by user_id
     ) t2;


-- 第二种：   断一天也算连续
select
    user_id,finalflag
from (
         select user_id,
                concat_ws('', collect_list(flag)) finalflag
         from (select user_id, -- 下单日期
                      dt,
                      if(datediff(dt, lag(dt, 1, dt) over (partition by user_id order by dt)) < 3, '1', '0') flag

               from dws_trade_user_order_1d
               where dt > date_sub('2020-06-15', 7)
              ) t1
         group by user_id
     ) t2
where finalflag like '%111%';


/*
    sql运行顺序：

            select
                xxx,xxxx
            from  A  join  B
            on A.XX  = B.xx
            where xxxx
            group by xxx
            having xxxx

        where 中能在Join前运行的一定会先运行。  谓词下推。
        先运行Join,再运行where中必须在Join后运行的过滤
        之后是group by
        having
        最后是select
 */
 explain
 select recent_days,
                count(*) new_user_count
         from dwd_user_register_inc
                  -- ②复制3份
                  lateral view explode(`array`(1, 7, 30)) tmp as recent_days
-- ①取最近30天所有用户注册的数据
         where dt > date_sub('2020-06-14', 30)
            and user_id = '200'
-- ③过滤出要统计的时间周期范围内的数据
           and dt > date_sub('2020-06-14', recent_days)
-- ④将过滤后的复制了3份的数据集按照 recent_days分组
         group by recent_days;

select  date_sub('2020-06-14', 30);
/*
rawDataSize         ,380899
totalSize           ,15987

 */
desc formatted dwd_user_register_inc;
desc formatted dwd_user_register_inc partition (dt = '2020-06-14');


/*
    重复购买(下单)人数 占 购买（下单）人数  比例

    user_id  brand   sku
        jack  nike   nike13
        tom   nike   nike12
        jack  nike   nike14

    人数：  2
    人次：  3

 */
select dt,
       recent_days,
       tm_id,
       tm_name,
       order_repeat_rate
from ads_repeat_purchase_by_tm;

 insert overwrite table ads_repeat_purchase_by_tm
     select * from ads_repeat_purchase_by_tm
     union 
select
       '2020-06-14',
        30,
        tm_id,tm_name,
        cast(sum(`if`(total_order_count_30d > 1,1,0)) / count(*) * 100 as decimal(16,2)) order_repeat_rate
from (
         select user_id,
                tm_id,
                tm_name,
                sum(order_count_30d) total_order_count_30d
-- 一个人下单一种sku是一行
         from dws_trade_user_sku_order_nd
         where dt = '2020-06-14'
-- 先统计出每个人下单每种品牌的次数
         group by user_id, tm_id
                -- tm_name 和 tm_id是1对1的关系
                , tm_name
     ) t1
group by tm_id,tm_name;


select dt,
       recent_days,
       tm_id,
       tm_name,
       order_count,
       order_user_count
from ads_trade_stats_by_tm;


 insert overwrite table ads_trade_stats_by_tm
     select * from ads_trade_stats_by_tm
     union
-- 求最近1天各品牌的下单人数和下单数
select
       '2020-06-14',
      1 recent_days,
    tm_id,tm_name,
       sum(order_count_1d) order_count,
       count(distinct user_id) order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_1d
where dt='2020-06-14'
group by tm_id,tm_name

union all
-- 求最近7,30天各品牌的下单人数和下单数
-- 如果一条记录的 order_count_30d > 0 ,这条记录的 order_count_7d 是否一定也 > 0 ?
--  jack最近30天下单了 iphone13，是否最近7天也下单了 iphone13? 不一定。 每一行数据的 order_count_7d，有可能为0
select
        '2020-06-14',recent_days,
    tm_id,tm_name,
       sum(`if`(recent_days = 7 , order_count_7d,order_count_30d)) order_count,
       -- 先判断这个人是否买过
       -- 特殊处理： 如果一个品牌在最近7天没人买过，那么count(null) = null，进行空值处理
       nvl(count(distinct  `if`(recent_days = 7 and order_count_7d = 0,null,user_id)),0)  order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_nd
lateral view explode(`array`( 7, 30)) tmp as recent_days
where dt='2020-06-14'
group by tm_id,tm_name,recent_days;



select dt,
       recent_days,

       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,

       order_count,
       order_user_count
from ads_trade_stats_by_cate;


 insert overwrite table ads_trade_stats_by_cate
     select * from ads_trade_stats_by_cate
     union
-- 求最近1天各品类的下单人数和下单数
select
       '2020-06-14',
      1 recent_days,
     category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
       sum(order_count_1d) order_count,
       count(distinct user_id) order_user_count
from dws_trade_user_sku_order_1d
where dt='2020-06-14'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name
            -- 和category3_id 都是 1:1的关系，直接加在group by 后面

union all
select
        '2020-06-14',recent_days,
    category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,
       sum(`if`(recent_days = 7 , order_count_7d,order_count_30d)) order_count,
       -- 先判断这个人是否买过
       -- 特殊处理： 如果一个品牌在最近7天没人买过，那么count(null) = null，进行空值处理
       nvl(count(distinct  `if`(recent_days = 7 and order_count_7d = 0,null,user_id)),0)  order_user_count
       -- 一个用户下单一个sku是一行
from dws_trade_user_sku_order_nd
lateral view explode(`array`( 7, 30)) tmp as recent_days
where dt='2020-06-14'
group by category3_id,category3_name,category2_id,category2_name,category1_id,category1_name,recent_days;


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
from  ads_sku_cart_num_top3_by_cate;

 insert overwrite table ads_sku_cart_num_top3_by_cate
     select * from ads_sku_cart_num_top3_by_cate
     union 
select
   '2020-06-14' dt,
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
                  where dt = '2020-06-14'
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
                  where dt = '2020-06-14'
              ) t2
              on t1.sku_id = t2.id
     ) t3
where rk <= 3;


select dt,
       tm_id,
       tm_name,
       sku_id,
       sku_name,
       favor_count,
       rk
from ads_sku_favor_count_top3_by_tm;


 insert overwrite table ads_sku_favor_count_top3_by_tm
     select * from ads_sku_favor_count_top3_by_tm
     union 
select
     '2020-06-14' dt,
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
         where dt = '2020-06-14'
     ) t1
where  rk <= 3;




select dt,
       order_to_pay_interval_avg
from ads_order_to_pay_interval_avg;

/*
        想要指定的类型时：
                a:用构造器(x)
                b: cast(x as 类型)
 */
  insert overwrite table ads_order_to_pay_interval_avg
      select * from ads_order_to_pay_interval_avg
      union 
select
        '2020-06-14',
        bigint(avg(to_unix_timestamp(payment_time) -  to_unix_timestamp(order_time)))
-- 分区：按照订单是否完成(确认收货)进行分区，未确认收货的在9999-12-31分区，已经确认收货的，按照收货日期分区
from dwd_trade_trade_flow_acc
-- 取最近1日，所有已经支付的订单的数据。
-- 最近1日，所有支付的订单，可能确认收货了，在 2020-06-14分区
-- 最近1日，所有支付的订单，未确认收货，在 9999-12-31分区
where (dt = '9999-12-31' or dt='2020-06-14')
-- 只要支付的订单
  -- isnotnull(a) 等价于 a is not null
and isnotnull(payment_time);


select to_unix_timestamp('2020-06-10 21:52:48');


select dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_code_3166_2,
       order_count,
       order_total_amount
from ads_order_by_province;

 insert overwrite table ads_order_by_province
     select * from ads_order_by_province
     union 
-- 各省份最近1日的
select
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
where dt='2020-06-14'
union all
-- 各省份最近7，30日的
select
          recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2 iso_code_3166_2,
       max(`if`(recent_days = 7 ,order_count_7d ,order_count_30d))  order_count,
       sum(`if`(recent_days = 7 ,order_total_amount_7d ,order_total_amount_30d))  order_total_amount
-- 1个省份是一行
from dws_trade_province_order_nd
lateral view explode(`array`( 7, 30)) tmp as recent_days
where dt='2020-06-14'
group by recent_days,province_id,
         -- 这些字段和 province_id 是1：1关系
          province_name,
       area_code,
       iso_code,
       iso_3166_2;




select dt,
       coupon_id,
       coupon_name,
       used_count,
       used_user_count
from ads_coupon_stats;

-- false
set hive.merge.sparkfiles=true;

 insert overwrite table ads_coupon_stats
     select * from ads_coupon_stats
     union 
select
      '2020-06-14' dt,
       coupon_id,
       coupon_name,
       sum(used_count_1d) used_count,
       count(*) used_user_count
-- 一个用户，使用的一种coupon_id是一行
from dws_tool_user_coupon_coupon_used_1d
where dt='2020-06-14'
group by coupon_id,coupon_name;

show tables in gmall like 'ads*';

show tables ;

show create table dim_coupon_full;

-- insert overwrite dim_coupon_full partiton(dt='2020-06-14) select * xxxx
-- 查元数据:    'hdfs://hadoop102:9820/warehouse/gmall/dim/dim_coupon_full/dt=2020-06-14
--  file path not exists
show partitions dim_coupon_full;


-------------脚本处理-----------------------
sed -i.bak -e 's/2020-06-14/$do_date/g' -e 's/2020-06-15/$do_date/g' -e 's/`/\\`/g'  ads层脚本.sh

/*
        假设所有的脚本都需要按照以上规则进行替换，脚本放在 /home/atguigu/scripts220309
 */
#!bin/bash
scriptsPath=/home/atguigu/scripts220309
# 列出这个目录中所有的文件名
for file in `ls $/home/atguigu/scripts220309`
do
    sed -i.bak -e 's/2020-06-14/$do_date/g' -e 's/2020-06-15/$do_date/g' -e 's/`/\\`/g'  $scriptsPath/$file
done

select split(space(99)," ")

