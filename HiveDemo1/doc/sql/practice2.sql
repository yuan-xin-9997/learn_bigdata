-- ① 创建原始数据表gulivideo_ori
create external table gulivideo_ori(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>
)
row format delimited fields terminated by "\t"
collection items terminated by "&"
stored as textfile
location '/gulivideo/video';

-- ② 创建原始数据表: gulivideo_user_ori
create external table gulivideo_user_ori(
    uploader string,
    videos int,
    friends int
)
row format delimited 
fields terminated by "\t" 
stored as textfile
location '/gulivideo/user';

-- ③ 创建orc存储格式带snappy压缩的表gulivideo_orc：
create table gulivideo_orc(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>
)
stored as orc
tblproperties("orc.compress"="SNAPPY");

-- ④ 创建orc存储格式带snappy压缩的表gulivideo_user_orc：
create table gulivideo_user_orc(
    uploader string,
    videos int,
    friends int
)
row format delimited 
fields terminated by "\t" 
stored as orc
tblproperties("orc.compress"="SNAPPY");

-- ⑤ 向ori表插入数据
load data local inpath "/opt/module/hive/datas/video" into table gulivideo_ori;
load data local inpath "/opt/module/hive/datas/user.txt" into table gulivideo_user_ori;

-- ⑥ 向orc表插入数据
insert into table gulivideo_orc select * from gulivideo_ori;
insert into table gulivideo_user_orc select * from gulivideo_user_ori;



-- 统计硅谷影音视频网站的常规指标，各种TopN指标：
-- 统计视频观看数Top10
-- 统计视频类别热度Top10（类别热度：类别下的总视频数）
-- 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
-- 统计视频观看数Top50所关联视频的所属类别Rank（每个类别下有多少视频）
-- 统计每个类别中的视频热度Top10，以Music为例（视频热度：视频观看数）
-- 统计每个类别视频观看数Top10
-- 统计上传视频最多的用户Top10以及他们上传的视频观看次数在前20的视频



-- set hive.exec.mode.local.auto=true

-- 统计硅谷影音视频网站的常规指标，各种TopN指标：
-- 统计视频观看数Top10
select
    videoId,
    `views`
from gulivideo_orc
order by `views` limit 10;  -- 写法1，最简单粗暴的，但是生产不推荐，大数据量的情况下

select
    videoId,
    `views`
from gulivideo_orc
distribute by substring(videoId, 1, 2) sort by `views` limit 10; -- 写法2，推荐，使用分区



-- 统计视频类别热度Top10（类别热度：类别下的总视频数）
select
    videoId,
    category_name
from gulivideo_orc
lateral view explode(category) tmp_table as category_name;

select
    t1.category_name,
    count(t1.videoId) as video_count
from (
    select
    videoId,
    category_name
    from gulivideo_orc
    lateral view explode(category) tmp_table as category_name
) t1
group by t1.category_name;

select
    t2.category_name,
    t2.video_count
from (
    select
    t1.category_name,
    count(t1.videoId) as video_count
    from (
        select
        videoId,
        category_name
        from gulivideo_orc
        lateral view explode(category) tmp_table as category_name
    ) t1
    group by t1.category_name
) t2
order by t2.video_count desc limit 10;


-- 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
--- （1）统计出视频观看数最高的20个视频的所属类别
select
    videoId,
    `views`,
    category
from gulivideo_orc
order by `views` desc limit 20;

select
    category_name,
    t1.videoId
from (
    select
    videoId,
    `views`,
    category
from gulivideo_orc
order by `views` desc limit 20
     ) t1
lateral  view explode(t1.category) tmp_table as category_name;

---(3)类别包含Top20视频的个数
select
    t2.category_name,
    count(t2.videoId) video_count
from (
        select
        category_name,
        t1.videoId
    from (
        select
        videoId,
        `views`,
        category
    from gulivideo_orc
    order by `views` desc limit 20
         ) t1
    lateral  view explode(t1.category) tmp_table as category_name
) t2
group by t2.category_name;

-- 统计视频观看数Top50所关联视频的所属类别Rank（每个类别下有多少视频）
--- (1) 统计视频观看数Top50所关联视频
select
    videoId,
    `views`,
    relatedId
from gulivideo_orc
order by `views` desc
limit 50;

select
    distinct related_id
from (select videoId,
             `views`,
             relatedId
      from gulivideo_orc
      order by `views` desc
      limit 50) t1
lateral view explode(t1.relatedId) tmp_table as related_id;


---（2）关联视频的所属类别Rank（每个类别下有多少视频）
select
    go.videoId,
    go.category
from (
    select
    distinct related_id
    from (select videoId,
                 `views`,
                 relatedId
          from gulivideo_orc
          order by `views` desc
          limit 50) t1
    lateral view explode(t1.relatedId) tmp_table as related_id
     ) t2 join gulivideo_orc go on t2.related_id=go.videoId;

select
    category_name,
    t3.videoId
from (
    select
    go.videoId,
    go.category
    from (
        select
        distinct related_id
        from (select videoId,
                     `views`,
                     relatedId
              from gulivideo_orc
              order by `views` desc
              limit 50) t1
        lateral view explode(t1.relatedId) tmp_table as related_id
         ) t2 join gulivideo_orc go on t2.related_id=go.videoId
) t3
lateral view
explode(t3.category) tmp_table as category_name;


select
    t4.category_name,
    count(t4.videoId) count_video
from (
    select
    category_name,
    t3.videoId
    from (
        select
        go.videoId,
        go.category
        from (
            select
            distinct related_id
            from (select videoId,
                         `views`,
                         relatedId
                  from gulivideo_orc
                  order by `views` desc
                  limit 50) t1
            lateral view explode(t1.relatedId) tmp_table as related_id
             ) t2 join gulivideo_orc go on t2.related_id=go.videoId
    ) t3
    lateral view
    explode(t3.category) tmp_table as category_name
     ) t4
group by t4.category_name
;

select
    t5.category_name,
    t5.count_video,
    rank() over (order by t5.count_video desc) rk
from (
    select
    t4.category_name,
    count(t4.videoId) count_video
    from (
        select
        category_name,
        t3.videoId
        from (
            select
            go.videoId,
            go.category
            from (
                select
                distinct related_id
                from (select videoId,
                             `views`,
                             relatedId
                      from gulivideo_orc
                      order by `views` desc
                      limit 50) t1
                lateral view explode(t1.relatedId) tmp_table as related_id
                 ) t2 join gulivideo_orc go on t2.related_id=go.videoId
        ) t3
        lateral view
        explode(t3.category) tmp_table as category_name
         ) t4
    group by t4.category_name
) t5;

-- 统计每个类别中的视频热度Top10，以Music为例（视频热度：视频观看数）
-- (1) 炸裂category
select
    videoId,
    `views`,
    category_name
from gulivideo_orc
lateral view
explode(category) tmp_table as category_name;
-- (2) where catergory_name='Music' order by views desc limit 10
select
    t1.videoId,
    t1.`views`
from(
    select
    videoId,
    `views`,
    category_name
    from gulivideo_orc
    lateral view
    explode(category) tmp_table as category_name
) t1 where t1.category_name='Music' order by t1.views desc limit 10;

-- 统计每个类别视频观看数Top10
--- (1)炸裂
select
    videoId,
    `views`,
    category_name
from gulivideo_orc
lateral view
explode(category) tmp_table as category_name;
--- (2)对炸裂结果进行开窗排序
select
    t1.category_name,
    t1.videoId,
    t1.views,
    rank() over (partition by t1.category_name order by t1.views) rk
from
(
    select
    videoId,
    `views`,
    category_name
    from gulivideo_orc
    lateral view
    explode(category) tmp_table as category_name
) t1
;
--- (3)过滤rk<=10
select
    *
from
(
    select
    t1.category_name,
    t1.videoId,
    t1.views,
    rank() over (partition by t1.category_name order by t1.views) rk
    from
    (
        select
        videoId,
        `views`,
        category_name
        from gulivideo_orc
        lateral view
        explode(category) tmp_table as category_name
    ) t1
) t2
where t2.rk <= 10;

-- 统计上传视频最多的用户Top10以及他们上传的视频观看次数在前20的视频





