
-- 建大表、小表和JOIN后表的语句
create table bigtable(id bigint, t bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';
create table smalltable(id bigint, t bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';
create table jointable(id bigint, t bigint, uid string, keyword string, url_rank int, click_num int, click_url string) row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/bigtable' into table bigtable;
load data local inpath '/opt/module/hive/datas/smalltable' into table smalltable;



Explain
select b.id, b.t, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from smalltable s
 join bigtable b
on s.id = b.id;

Explain
select b.id, b.t, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from
     bigtable b
    join
    smalltable s
on s.id = b.id;

-- 大表、大表SMB JOIN（重点）
-- 1. SMB： sort merge bucket join

-- 对照案例，普通大表join
-- ① 创建第二张大表bigtable2，并加载数据
create table bigtable2(
    id bigint,
    t bigint,
    uid string,
    keyword string,
    url_rank int,
    click_num int,
    click_url string)
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/bigtable' into table bigtable2;

insert overwrite table jointable

-- 测试大表直接join
select b.id, b.t, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from bigtable s
join bigtable2 b
on b.id = s.id;
-- Time taken: 53.467 seconds

--  2）SMB案例，分桶大表join
-- ① 创建分桶表1--> bigtable_buck1,桶的个数不要超过可用CPU的核数
create table bigtable_buck1(
    id bigint,
    t bigint,
    uid string,
    keyword string,
    url_rank int,
    click_num int,
    click_url string)
clustered by(id)
sorted by(id)
into 24 buckets   -- 桶的个数和CPU核数和Reduce数需要一致
row format delimited fields terminated by '\t';

create table bigtable_buck2 like bigtable_buck1;

insert into bigtable_buck2 select * from bigtable;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- 测试SMB Join
insert overwrite table jointable
select b.id, b.t, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from bigtable_buck1 s
join bigtable_buck2 b
on b.id = s.id;
-- Time taken: 39.047 seconds


