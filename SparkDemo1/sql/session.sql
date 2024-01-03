-- day01 用户行为轨迹分析
-- 需求：分析用户每个会话［上一次访问与本次访问是否超过半小时，如果超过则是新会话］的行为轨迹
create table user_session(
    user_id string,
    action_time string,
    page string
)
row format delimited fields terminated by ',';

select * from user_session;

-- 使用窗口函数lag获取每条访问记录上一次访问的时间
select
    user_id,
    action_time,
    page,
    lag(action_time) over (partition by user_id order by action_time) before_action_time
from user_session;

-- 使用if函数将访问时间与上一次访问时间超过30秒，并且上一次访问时间为null的记录进行筛选处理，新增的列有值的则表示为一次新的会话
select
    user_id,
    action_time,
    page,
    `if`(before_action_time is null or unix_timestamp(action_time) - unix_timestamp(before_action_time) > 30*60, concat(user_id, unix_timestamp(action_time)), null)
from (
    select
    user_id,
    action_time,
    page,
    lag(action_time) over (partition by user_id order by action_time) before_action_time
from user_session
     ) t1;

-- 使用last_value窗口函数对session_point进行开窗，将session_point为空的列，用第一条记录填充（是为session id）
select
    user_id, action_time, page,
    last_value(session_point, true) over (partition by user_id order by action_time) session_id
from (

    select
    user_id,
    action_time,
    page,
    `if`(before_action_time is null or unix_timestamp(action_time) - unix_timestamp(before_action_time) > 30*60, concat(user_id, unix_timestamp(action_time)), null) session_point
from (
    select
    user_id,
    action_time,
    page,
    lag(action_time) over (partition by user_id order by action_time) before_action_time
from user_session
     ) t1
     ) t2;

-- 使用row_number窗口函数计算每次session的访问次序
select
    user_id, action_time, page,
    row_number() over (partition by session_id order by action_time) action_rk
from (
    select
    user_id, action_time, page,
    last_value(session_point, true) over (partition by user_id order by action_time) session_id
from (

    select
    user_id,
    action_time,
    page,
    `if`(before_action_time is null or unix_timestamp(action_time) - unix_timestamp(before_action_time) > 30*60, concat(user_id, unix_timestamp(action_time)), null) session_point
from (
    select
    user_id,
    action_time,
    page,
    lag(action_time) over (partition by user_id order by action_time) before_action_time
from user_session
     ) t1
     ) t2
     ) t3;