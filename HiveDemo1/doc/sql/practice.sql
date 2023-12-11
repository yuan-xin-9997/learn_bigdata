set hive.exec.mode.local.auto= true;


-- 第1题
-- 1. 表结构：表名：score_info   字段：uid、subject_id,score
create table score_info(
    uid string,          -- 学生id
    subject_id string, -- 课程id
    score int            -- 课程分数
)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/hive/datas/score_info.txt' into table score_info;

-- 2. 需求：找出所有科目成绩都大于该学科平均成绩的学生

select
    uid,
    subject_id,
    score,
    avg(score) over(partition by subject_id ) sub_avg
from score_info;
/**
  上述SQL执行结果，先使用开窗函数将各学科的平均分进行计算，并新增一列sub_avg
uid	subject_id	score	sub_avg
1003	01	70	81.66666666666667
1002	01	85	81.66666666666667
1001	01	90	81.66666666666667
1003	02	70	81.66666666666667
1002	02	85	81.66666666666667
1001	02	90	81.66666666666667
1003	03	85	81.66666666666667
1002	03	70	81.66666666666667
1001	03	90	81.66666666666667
 */

select
    t.uid, subject_id, score, sub_avg,
    if(score>sub_avg, 1, 0) flag
from (
    select
    uid,
    subject_id,
    score,
    avg(score) over(partition by subject_id ) sub_avg
from score_info
) t;
/**
上述SQL的执行结果，在上述的基础上，使用if函数对每个学生的得分与平均分进行比较，新增flag列，如果大于，则flag=1，否则=0
t.uid	subject_id	score	sub_avg	flag
1003	01	70	81.66666666666667	0
1002	01	85	81.66666666666667	1
1001	01	90	81.66666666666667	1
1003	02	70	81.66666666666667	0
1002	02	85	81.66666666666667	1
1001	02	90	81.66666666666667	1
1003	03	85	81.66666666666667	1
1002	03	70	81.66666666666667	0
1001	03	90	81.66666666666667	1
 */

select
    t1.uid,
    --subject_id, score, sub_avg, flag,
    sum(t1.flag) sum_flag
from (
    select
        t.uid, subject_id, score, sub_avg,
        if(score>sub_avg, 1, 0) flag
    from (
        select
        uid,
        subject_id,
        score,
        avg(score) over(partition by subject_id ) sub_avg
    from score_info
    ) t
) t1 group by  uid having sum_flag=3;

/**
上述SQL的执行结果，在上述的基础上，基于学生字段进行group by分组，并使用having进行过滤，过滤那些flag求和=3的学生
t1.uid	sum_flag
1001	3
 */



-- 第2题
-- 1. 数据结构：某平台的用户访问数据
-- 表名：action
-- 字段：userId，visitDate，visitCount
-- 2. 需求：要求使用SQL统计出每个用户的月累计访问次数及累计访问次数（注意日期数据的格式是否能解析），
-- 3. 数据准备:
create table action(
    userId string,    -- 用户id
    visitDate string, -- 访问日期
    visitCount int     -- 访问次数
)
row format delimited fields terminated by "\t";

load data local inpath '/opt/module/hive/datas/action.txt' into table action;

-- 要求使用SQL统计出每个用户的月累计访问次数及累计访问次数（注意日期数据的格式是否能解析），

-- 使用regexp_replace函数时间字符串中的/转换为-，并使用date_format函数获取年月
select
    userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') `date`
from action;

-- 在上述基础上，对访问次数进行开窗，累加访问次数，对用户id、年月进行分区，并按照年月进行升序
select
    distinct userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') `date` ,
    sum(visitCount) over(
        partition by userId, date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
            order by date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
    ) month_count
from action;

-- (1) 要求使用SQL统计出每个用户的月累计访问次数、及逐月累计访问次数（注意日期数据的格式是否能解析）
with t as (
    select
    distinct userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') `date` ,
    sum(visitCount) over(
        partition by userId, date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
            order by date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
    ) month_count
    from action
)
select
    t.userId,
    t.`date`,
    t.month_count,
    sum(t.month_count) over(partition by t.userId order by t.`date`) visit_sum
from t;

-- （2）要求使用SQL统计出每个用户的月累计访问次数、及总累计访问次数
with t as (
    select
    distinct userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') `date` ,
    sum(visitCount) over(
        partition by userId, date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
            order by date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM')
    ) month_count
    from action
)
select
    t.userId,
    t.`date`,
    t.month_count,
    sum(t.month_count) over(partition by t.userId) visit_sum
from t;




-- 1.3 第3题
-- 1. 背景：
-- 有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时，都会产生一个访问日志，访问日志存储的表名
-- 2. 表名：visit
--    字段名：user_id,shop
-- 3. 需求:
-- ① 创建表
-- ② 每个店铺的UV（访客数）
-- ③ 每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
create table visit(
    user_id string,
    shop string
)
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/visit.txt'  into table visit;

-- 每个店铺的访问次数
select
    shop,
    count(user_id) uv
from visit
group by shop;

-- 每个店铺的访问次数（效果与上面group等价）
with t as (
    select
    shop,
    count(user_id) over(partition by shop) uv
    from visit
)
select distinct t.shop, t.uv
from t;


-- 每个店铺的访客数
select
    shop,
    count(distinct user_id) uv
from visit
group by shop;

-- 统计每个店铺，每个用户总访问次数，对shop,user_id进行开窗，开窗函数count
select
    shop,
    user_id,
    count(user_id) over (partition by shop, user_id) user_count
from visit;

-- 在上述基础上，进行分组，达到去重的效果
with t as (
    select
    shop,
    user_id,
    count(user_id) over (partition by shop, user_id) user_count
from visit
)
select
    t.shop,
    t.user_id,
    t.user_count
from t
group by t.shop, t.user_id, t.user_count ;

-- 在上述的基础上，增加排序函数，对用户访问次数进行排序，开窗函数rank，对商店进行分区，并对用户访问次数进行降序排序
with t as (
    select
    shop,
    user_id,
    count(user_id) over (partition by shop, user_id) user_count
from visit
)
select
    t.shop,
    t.user_id,
    t.user_count,
    rank() over (partition by t.shop order by t.user_count desc ) rk
from t
group by t.shop, t.user_id, t.user_count ;

-- 在上述基础上，嵌套查询，对排名小于3的进行筛选出来
select
    t2.shop, user_id, user_count, rk
from (
        with t as (
        select
        shop,
        user_id,
        count(user_id) over (partition by shop, user_id) user_count
    from visit
    )
    select
        t.shop,
        t.user_id,
        t.user_count,
        rank() over (partition by t.shop order by t.user_count desc ) rk
    from t
    group by t.shop, t.user_id, t.user_count
) t2
where t2.rk <= 3;

