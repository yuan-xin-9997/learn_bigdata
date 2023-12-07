-- 向分区表加载数据
load data local inpath '/opt/module/hive/datas/dept_20200401.log'
into table dept_partition partition(day='20200401');

load data local inpath '/opt/module/hive/datas/dept_20200402.log'
into table dept_partition partition(day='20200402');

load data local inpath '/opt/module/hive/datas/dept_20200403.log'
into table dept_partition partition(day='20200403');

-- 创建二级分区
create table dept_partition2(
       deptno int,
       dname string,
       loc string
)
partitioned by (day string, hour string)
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/dept_20200401.log'
into table dept_partition2 partition(day='20200401', hour='11');

load data local inpath '/opt/module/hive/datas/dept_20200402.log'
into table dept_partition2 partition(day='20200401', hour='12');

load data local inpath '/opt/module/hive/datas/dept_20200403.log'
into table dept_partition2 partition(day='20200401', hour='13');

-- 创建动态分区
create table dept_partition_dynamic(
       id int,
       name string
)
partitioned by (loc int)
row format delimited fields terminated by '\t';

insert into table dept_partition_dynamic partition(loc) select deptno, dname, loc from dept;

-- 创建分桶表
create table stu_bucket(id int, name string)
clustered by(id)
into 4 buckets
row format delimited fields terminated by '\t';

load data local inpath   '/opt/module/hive/datas/student.txt' into table stu_bucket;