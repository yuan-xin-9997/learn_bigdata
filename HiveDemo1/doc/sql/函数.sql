--- 8 函数

--- 8.2 常用内置函数
---- 8.2.1 空字段赋值---NVL
---- 查询emp_nvl表中ename、sal、comm 如果comm为null，用0代替
create table emp_nvl(
    id int,
    sal double,
    comm double
)
row format delimited fields terminated by ',';
load data local inpath '/opt/module/hive/datas/emp_nvl.txt' into table emp_nvl;

---- 查询emp表中员工的总收入
select id, sal, comm, sal+nvl(comm,0) from emp_nvl;

---- 8.2.2 CASE when then else end

---- 案例需求：求出每个部门男女各多少人？
----- 结果示例：
-- dept_id man_num woman_num
-- A       2       1
-- B       1       2
---- 创建表
create table emp_sex(
name string,
dept_id string,
sex string
)
row format delimited fields terminated by ",";
load data local inpath '/opt/module/hive/datas/emp_sex.txt' into table emp_sex;

---- 查询
select
    dept_id,
    sum(case sex when '男' then 1 else 0 end) man_num,
    sum(case sex when '女' then 1 else 0 end) woman_num
from emp_sex group by dept_id;

--  if函数
select id,sal,if(sal>2000,1,0) from emp_nvl;
select id,sal,if(sal<1500,1,if(sal<2000,2,3)) from emp_nvl;

---- 8.2.3 行转列
select concat(1,2,3,4);
select concat_ws(',','1','2','3','4');
select concat_ws(null,'1','2','3','4');
select concat_ws(',','1','2','3',null);

---- concat、concat_ws、collect_set
--- 案例需求：把星座和血型一样的人归类到一起
--- 结果示例：
-- 射手座,A            大海|凤姐
-- 白羊座,A            孙悟空|猪八戒
-- 白羊座,B            宋宋|苍老师
--- 创建表
create table person_info(
name string,
constellation string,
blood_type string
)
row format delimited fields terminated by ",";
load data local inpath '/opt/module/hive/datas/person_info.txt' into table person_info;

--- 查询
select
    t1.con_blo,
    concat_ws('|', collect_set(t1.name))
from (
    select
        name,
        concat_ws(',', constellation, blood_type) con_blo
    from
        person_info
) t1
group by con_blo
;

---- 8.2.4 列转行

---- explode、split、lateral view

---- 案例需求：将电影分类中的category列的数据展开
---- 结果示例：
-- 《疑犯追踪》      悬疑
-- 《疑犯追踪》      动作
-- 《疑犯追踪》      科幻
-- 《疑犯追踪》      剧情
-- 《Lie to me》   悬疑
-- 《Lie to me》   警匪
-- 《Lie to me》   动作
-- 《Lie to me》   心理
-- 《Lie to me》   剧情
-- 《战狼2》        战争
-- 《战狼2》        动作
-- 《战狼2》        灾难
--- 创建表
create table movie_info(
    movie string,
    category string)
row format delimited
fields terminated by "\t";
load data local inpath "/opt/module/hive/datas/movie_info.txt" into table movie_info;

--- 查询
select
    movie, catagory_name
from movie_info
lateral view
explode(split(category, ',')) tmp_table as catagory_name
;

---- 8.2.5 窗口函数

--- over()、lag()、 lead()、 ntile()

--- 数据准备

-- （1）查询在2017年4月份购买过的顾客及总人数
-- （2）查询顾客的购买明细及月购买总额
-- （3）上述的场景, 将每个顾客的cost按照日期进行累加
--- 3.1 所有行进行累加
--- 3.2 按照name 分组，组内数据累加
--- 3.3 按照name分区，组内数据按照日其有序累加
--- 3.4 按照name分区，组内数据按照日期排序，由起点到当前行进行累加
--- 3.5 按照name分区，组内数据按照日期排序，由前一行和当前行进行做聚合
--- 3.6 按照name分区，组内数据按照日期排序，由当前行和前一行和后一行做聚合
--- 3.6 按照name分区，组内数据按照日期排序，由当前行和后面所有行作聚合
-- （4）查询每个顾客上次的购买时间
-- （5）查询前20%时间的订单信息

---- 8.2.6 Rank
---- Rank()、Dense_rank()、row_number()
---数据准备
--- 计算每门学科成绩排名
