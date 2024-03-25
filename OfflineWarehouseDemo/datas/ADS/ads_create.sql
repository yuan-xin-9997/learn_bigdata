------------------------------------------- 11.1 流量主题 -------------------------------------------
-- 各渠道流量统计
-- 需求说明如下
-- 统计周期	统计粒度	指标	说明
-- 最近1/7/30日	渠道	访客数	统计访问人数
    -- 业务过程：页面浏览
    -- 访客是mid标识的，一个设备id相当于一个访客
    -- 聚合逻辑：count(distinct mid)
-- 最近1/7/30日	渠道	会话平均停留时长	统计所有会话平均停留时长
    -- 业务过程：页面浏览
    -- 聚合逻辑：avg(每个会话的总停留时间)
-- 最近1/7/30日	渠道	会话平均浏览页面数	统计每个会话平均浏览页面数
    -- 业务过程：页面浏览
    -- 聚合逻辑：avg(每个会话的总停留时间)
-- 最近1/7/30日	渠道	会话总数	统计会话总数
    -- 业务过程：页面浏览
    -- 聚合逻辑：count(distinct session_id)
-- 最近1/7/30日	渠道	跳出率	只有一个页面的会话的比例
    -- 业务过程：页面浏览
    -- 跳出率：访问页面总数为1的session个数/总session个数
    -- 聚合逻辑：

-- 建表语句
DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';

-- 最近1日指标
select
    '2020-06-14',
    1 recent_days,
    channel,
    count(distinct mid_id),
    avg(during_time_1d)/1000,
    avg(page_count_1d),
    count(1),
    sum(if(page_count_1d=1,1,0))/count(1)
from dws_traffic_session_page_view_1d where dt='2020-06-14';

-- 最近7日
select
    '2020-06-14',
    7 recent_days,
    channel,
    count(distinct mid_id),
    avg(during_time_1d)/1000,
    avg(page_count_1d),
    count(1),
    sum(if(page_count_1d=1,1,0))/count(1)
from dws_traffic_session_page_view_1d where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 6);

-- 最近30日
select
    '2020-06-14',
    30 recent_days,
    channel,
    count(distinct mid_id),
    avg(during_time_1d)/1000,
    avg(page_count_1d),
    count(1),
    sum(if(page_count_1d=1,1,0))/count(1)
from dws_traffic_session_page_view_1d where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 29);


-- 第一种方式
-- 问题：对dws_traffic_session_page_view_1d直接explode的时候，是将整表炸开，数据量后续会比较大，性能比较慢
select
    '2020-06-14',
    recent_days,
    channel,
    count(distinct mid_id),
    avg(during_time_1d)/1000,
    avg(page_count_1d),
    count(1),
    sum(if(page_count_1d=1,1,0))/count(1)
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 29)
group by channel,recent_days;

-- 第二种方式
-- 相比于第一种方式，炸开的数据量小一些
-- 注意插入数据的方式
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel where dt!='2020-06-14'
union
select
    '2020-06-14',
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
    count(distinct if(dt='2020-06-14', mid_id, null)) uv_count_1d,
    avg(if(dt='2020-06-14',during_time_1d,null))/1000 avg_duration_sec_1d,
    avg(if(dt='2020-06-14',page_count_1d,null)) avg_page_count_1d,
    sum(if(dt='2020-06-14' and page_count_1d=1, 1, 0)) sv_count_1d,
    sum(if(page_count_1d=1 and dt='2020-06-14',1,0))/sum(if(dt='2020-06-14' and page_count_1d=1, 1, 0)) bound_rate_1d,

    count(distinct if(dt>=date_sub('2020-06-14',6), mid_id, null)) uv_count_7d,
    avg(if(dt>=date_sub('2020-06-14',6),during_time_1d,null))/1000 avg_duration_sec_7d,
    avg(if(dt>=date_sub('2020-06-14',6),page_count_1d,null)) avg_page_count_7d,
    sum(if(dt>=date_sub('2020-06-14',6) and page_count_1d=1, 1, 0)) sv_count_7d,
    sum(if(page_count_1d=1 and dt>=date_sub('2020-06-14',6),1,0))/sum(if(dt>=date_sub('2020-06-14',6) and page_count_1d=1, 1, 0)) bound_rate_7d,

    count(distinct mid_id) uv_count_30d,
    avg(during_time_1d)/1000 avg_duration_sec_30d,
    avg(page_count_1d) avg_page_count_30d,
    count(1) sv_count_30d,
    sum(if(page_count_1d=1,1,0))/count(1) bound_rate_30d
from dws_traffic_session_page_view_1d
where dt<='2020-06-14' and dt>=date_sub('2020-06-14', 29)
group by channel) t1 lateral view explode(array(1,7,30)) tmp as recent_days;



-- 11.1.2 路径分析
-- 用户路径分析，顾名思义，就是指用户在APP或网站中的访问路径。为了衡量网站优化的效果或营销推广的效果，以及了解用户行为偏好，时常要对访问路径进行分析。
-- 用户访问路径的可视化通常使用桑基图。如下图所示，该图可真实还原用户的访问路径，包括页面跳转和页面访问次序。
-- 桑基图需要我们提供每种页面跳转的次数，每个跳转由source/target表示，source指跳转起始页面，target表示跳转终到页面。

/*
ADS建表套路：
    1. 分析当前表数据的导入源、输入
    2. 当前表要求的输出、粒度、行数
    3. 结合1,2，推理计算过程
        a. 目标表取数据，在取数据之前，搞清楚目标表的粒度
        b. 搞清楚目标表如何分区

ads_page_path和业务数据无关，和日志log数据有关
优先去dws层(汇总层)，如果dws建模不够完善，会导致ADS需求找不到对应的DWS表，只能回溯DWD
*/

-- 1）建表语句
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`         STRING COMMENT '统计日期',
    `source`     STRING COMMENT '跳转起始页面ID',
    `target`     STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';
DROP TABLE IF EXISTS ads_user_change;

-- 2）数据装载
insert overwrite table ads_page_path -- 为了避免小文件，所以使用覆盖，而不是insert into
select *
from ads_page_path
union
-- 使用union为了实现幂等性，union=union all + group by 全字段
select '2020-06-14' dt,
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
            where dt = '2020-06-14' -- 按照访问日期分区;
           ) t1) t2
group by source, target;


-- 答疑


------------------------------------------- 11.2 用户主题 -------------------------------------------
-- 11.2.1 用户变动统计
-- 该需求包括两个指标，分别为流失用户数和回流用户数，以下为对两个指标的解释说明。
-- 指标	    说明
-- 流失用户数	之前活跃过的用户，最近一段时间未活跃，就称为流失用户。此处要求统计7日前（只包含7日前当天）活跃，但最近7日未活跃的用户总数。
-- 回流用户数	之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。
-- 1）建表语句
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数: 截止今天，所有用户中最后一次登录日期距离今天刚好7天的用户',
    `user_back_count`  BIGINT COMMENT '回流用户数：今天登录且截止到昨天最近连续7天及以上未活跃用户'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';
-- 2）数据装载
/*
表格1：m行 a列
表格2：n行，b列
表格1 笛卡尔积 表格2，结果：m+n行，a+b列
*/
select
    '2020-06-14' dt,
user_churn_count,
user_back_count
from (
-- user_churn_count
         select count(user_id) user_churn_count
         from dws_user_user_login_td --粒度：一个用户是一行，粒度：每天都有一个分区，这个分区存放的是截止到这一天所有的用户
         where dt = '2020-06-14'
           and datediff('2020-06-14', login_date_last) = 7) t3
         join (
-- user_back_count
    select count(*) user_back_count
    from (
-- 今天登录用户
             select user_id
             from dws_user_user_login_td
             where dt = '2020-06-14'
               and login_date_last = '2020-06-14') t1
             join (

-- 截止到昨天最近连续7天及以上未活跃用户
        select user_id
        from dws_user_user_login_td
        where dt = date_sub('2020-06-14', 1)
          and login_date_last <= date_sub('2020-06-14', 8)) t2
                  on t1.user_id = t2.user_id) t4;

--
-- 11.2.2 用户留存率
-- 留存分析一般包含新增留存和活跃留存分析。
-- 新增留存分析是分析某天的新增用户中，有多少人有后续的活跃行为。活跃留存分析是分析某天的活跃用户中，有多少人有后续的活跃行为。
-- 留存分析是衡量产品对用户价值高低的重要指标。
-- 此处要求统计新增留存率，新增留存率具体是指留存用户数与新增用户数的比值，例如2020-06-14新增100个用户，1日之后（2020-06-15）这100人中有80个人活跃了，那2020-06-14的1日留存数则为80，2020-06-14的1日留存率则为80%。
-- 要求统计每天的1至7日留存率，如下图所示。

-- 1）建表语句
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';
-- 2）数据装载


















