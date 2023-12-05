-- 创建student表
create table student_in(
    id int,
    name string
)
row format delimited
fields terminated by '\t';

-- 加载数据
load data local inpath '/opt/module/hive/datas/student.txt' into table student_in;

-- 创建student2表 要求：id仅为偶数的
create table student2 as select id,name from student_in where id % 2=0;

-- 创建student3表 要求：仅表结构和Student_in想通
create table student3 like student_in;

-- 创建外部表 teacher
create external table teacher(
    id int,
    name string
)
row format delimited
fields terminated by '\t';


-- 加载数据
load data local inpath '/opt/module/hive/datas/teacher.txt' into table teacher;
