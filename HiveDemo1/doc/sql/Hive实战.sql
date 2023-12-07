-- 数据准备
-- 创建orc存储格式带snappy压缩的表gulivideo_orc
create table gulivideo_orc(
  videoId string,  uploader string,
  age int,
  category array<string>,
  length int,
  views int,
  rate float,
  ratings int,
  comments int,
  relatedId array<string>)
stored as orc
tblproperties("orc.compress"="SNAPPY");

-- 创建orc存储格式带snappy压缩的表gulivideo_user_orc
create table gulivideo_user_orc(
   uploader string,
   videos int,
   friends int)
row format delimited
    fields terminated by "\t"
stored as orc
tblproperties("orc.compress"="SNAPPY");



-- 统计硅谷影音视频网站的常规指标，各种TopN指标：
-- 统计视频观看数Top10

-- 统计视频类别热度Top10

-- 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数

-- 统计视频观看数Top50所关联视频的所属类别Rank

-- 统计每个类别中的视频热度Top10,以Music为例

-- 统计每个类别视频观看数Top10

-- 统计上传视频最多的用户Top10以及他们上传的视频观看次数在前20的视频


