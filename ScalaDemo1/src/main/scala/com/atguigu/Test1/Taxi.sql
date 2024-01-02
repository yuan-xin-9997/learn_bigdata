-- 案例2：统计每个区域的平均等客时间【使用 hive sql+scala分别实现】

/*

A,龙华区,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02
B,宝安区,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45
A,龙岗区,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23
B,福田区,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33
A,龙岗区,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15
A,宝安区,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50
B,龙华区,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00
A,龙华区,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35
B,宝安区,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31
A,宝安区,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21
B,福田区,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12
B,龙岗区,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25

*/

-- 创建表
create table if not exists taxi(
    user_id string,
    from_region string,
    to_region string,
    from_time string,
    to_time string
)
row format delimited fields terminated by ",";

select * from taxi;

-- 导入数据
load data local inpath "/opt/module/scala" overwrite into table taxi;

select * from taxi;



-- 实现业务需求：统计每个区域的平均等客时间

    -- 得到每个司机每次的等客时间和等客区域
        -- 使用lead开窗函数得到下一次上车时间
select
    user_id,
    from_region,
    to_region,
    from_region,
    from_time,
    to_time,
    lead(from_time) over (partition by user_id order by from_time asc ) next_from_time
from taxi;


    -- 按照等客区域分组，计算平均值
select
    to_region,
    avg(unix_timestamp(next_from_time) - unix_timestamp(to_time)) avg_duration_time
from (
    select
        user_id,
        from_region,
        to_region,
        from_time,
        to_time,
        lead(from_time) over (partition by user_id order by from_time asc ) next_from_time
    from taxi
) t1
where next_from_time is not null
group by to_region
;

-- 结果排序
select *
from (
    select
    to_region,
    avg(unix_timestamp(next_from_time) - unix_timestamp(to_time)) avg_duration_time
from (
    select
        user_id,
        from_region,
        to_region,
        from_time,
        to_time,
        lead(from_time) over (partition by user_id order by from_time asc ) next_from_time
    from taxi
) t1
where next_from_time is not null
group by to_region
     ) t2
sort by t2.avg_duration_time desc ;