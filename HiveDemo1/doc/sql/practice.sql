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