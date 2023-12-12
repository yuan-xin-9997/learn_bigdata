-- 设置hive环境
set mapreduce.job.reduces=3;
set hive.auto.convert.join=false;

-- 创建name表
create table name(
    house_id int,
    name string
)
row format delimited
fields terminated by ','
tblproperties('bucketing_version'='3'); -- 设置哈希算法

-- 创建house表
create table house(
    id int,
    house_name string
)
row format delimited
fields terminated by ','
tblproperties('bucketing_version'='3'); -- 设置哈希算法

-- 加载数据
load data local inpath '/opt/module/hive/datas/name.txt' into table name;
load data local inpath '/opt/module/hive/datas/house.txt' into table house;


----------------优化前

-- join结果输出到文件
insert overwrite local directory '/opt/module/hive/datas/test01'
row format delimited fields terminated by '\t'
select
    n.house_id,
    n.name,
    h.house_name
from name n
join house h on h.id = n.house_id;

/**
  [atguigu@hadoop102 test01]$ cat 000000_0
12	凡凡	五号
[atguigu@hadoop102 test01]$ cat 000001_0
10	钱八	三号
10	田七	三号
10	赵六	三号
10	王五	三号
10	李四	三号
10	张三	三号
[atguigu@hadoop102 test01]$ cat 000002_0
11	吴签	四号

  结果已经很明显，出现了数据倾斜
 */

----------------优化后（大表打散，小表扩容）

-- 大表打散
select
    *,
    concat(house_id, '_', `floor`((rand()*10)%3)) pin_id
from name;

create table name_test
tblproperties('bucketing_version'='3')
as
select
    *,
    concat(house_id, '_', `floor`((rand()*10)%3)) pin_id
from name;
;

-- 小表扩容
select *, concat(id, '_', '0') pin_id from house;
select *, concat(id, '_', '1') pin_id from house;
select *, concat(id, '_', '2') pin_id from house;

select *, concat(id, '_', '0') pin_id from house union all
select *, concat(id, '_', '1') pin_id from house union all
select *, concat(id, '_', '2') pin_id from house;


create table house_test
tblproperties('bucketing_version'='3')
as
select *, concat(id, '_', '0') pin_id from house union all
select *, concat(id, '_', '1') pin_id from house union all
select *, concat(id, '_', '2') pin_id from house;

insert overwrite local directory '/opt/module/hive/datas/test02'
row format delimited fields terminated by '\t'
select
    n.house_id,
    n.name,
    h.house_name,
    h.pin_id,
    hash(h.pin_id) %3 parNum
from name_test n
join house_test h on h.id = n.house_id;

