create table order_tab(
    dt string,
    order_id string,
    user_id string,
    amount double)
    row format delimited fields terminated by ',';

--② 给出 2017年每个月的订单数、用户数、总成交金额。
select
    month(dt) month,
    count(order_id) order_count,
    count(distinct user_id) user_count,
    sum(amount)   amount_sum
from order_tab
where year(dt)='2017'
group by month(dt) ;

--③ 给出2017年11月的新客数（指在11月才有第一笔订单）

-- 2017-10-11  3   3000
-- 2017-11-12  3   4000

-- 1. 按照名字分区，按照日期排序 升序 为每个数据新增一个字段 min(dt)

select
    user_id,
    dt,
    min(dt) over(partition by user_id order by dt ) dt_min
from order_tab;

select
    count(distinct user_id) newUserNum
from (
         select
             user_id,
             dt,
             min(dt) over(partition by user_id order by dt ) dt_min
         from order_tab
         ) t1
where date_format(t1.dt_min ,'yyyy-MM')='2017-11';


