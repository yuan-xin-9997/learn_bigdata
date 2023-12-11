
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