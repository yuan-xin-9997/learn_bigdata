
create database  test_db;
use test_db;


CREATE TABLE IF NOT EXISTS test_db.example_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=OLAP
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2025-01-01 12:00:00"
);


CREATE TABLE IF NOT EXISTS test_db.example_list_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) NOT NULL COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
PARTITION BY LIST(`city`)
(
    PARTITION `p_cn` VALUES IN ("Beijing", "Shanghai", "Hong Kong"),
    PARTITION `p_usa` VALUES IN ("New York", "San Francisco"),
    PARTITION `p_jp` VALUES IN ("Tokyo")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2025-01-01 12:00:00"
);

 show partitions from example_range_tbl;

insert into test_db.example_range_tbl values (10000,'2017-01-01','北京',20,0,'2017-01-01 06:00:00',20,10,10);
select * from example_range_tbl;

alter table example_range_tbl add partition p201712 values less than ('2017-12-01');
alter table example_range_tbl drop partition p201703;

insert into test_db.example_range_tbl values (20000,'2017-11-01','北京',20,0,'2017-11-01 06:00:00',20,10,10);


show tables ;

select * from example_list_tbl;

show partitions from example_list_tbl;



create table student_dynamic_partition1
(
id int,
time date,
name varchar(50),
age int
)
duplicate key(id,time)
PARTITION BY RANGE(time)()
DISTRIBUTED BY HASH(id) buckets 10
PROPERTIES(
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "3",
"dynamic_partition.start" = "-7",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "10",
 "replication_num" = "1"
 );

SHOW DYNAMIC PARTITION TABLES;

CREATE TABLE IF NOT EXISTS test_db.example_site_visit
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
`last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10;

use test_db;
desc example_site_visit all;

insert into test_db.example_site_visit values
(10000,'2017-10-01','北京',20,0,'2017-10-01 06:00:00' ,20,10,10),
(10000,'2017-10-01','北京',20,0,'2017-10-01 07:00:00',15,2,2),
(10001,'2017-10-01','北京',30,1,'2017-10-01 17:05:45',2,22,22),
(10002,'2017-10-02','上海',20,1,'2017-10-02 12:59:12' ,200,5,5),
(10003,'2017-10-02','广州',32,0,'2017-10-02 11:20:00',30,11,11),
(10004,'2017-10-01','深圳',35,0,'2017-10-01 10:00:15',100,3,3),
(10004,'2017-10-03','深圳',35,0,'2017-10-03 10:20:22',11,6,6);

alter table example_site_visit add rollup rollup_cost_userid(user_id,cost);

explain select user_id,sum(cost) from example_site_visit group by user_id;
explain select city,sum(cost) from example_site_visit group by city;

alter table example_site_visit add rollup rollup_city_age_cost_maxd_mind(city,age,cost,max_dwell_time,min_dwell_time);
select * from example_site_visit;


select * from test_db.example_log;

CREATE TABLE IF NOT EXISTS test_db.example_log
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DUPLICATE KEY(`timestamp`, `type`)
DISTRIBUTED BY HASH(`timestamp`) BUCKETS 10;

alter table example_log add rollup rollup_types(type, timestamp, error_code, error_msg, op_id, op_time);

SHOW ALTER TABLE ROLLUP;

explain select * from example_log where timestamp='2020-10-01 00:00:00';
explain select * from example_log where type=1;

desc test_db.example_log all;

create table sales_records(
record_id int,
  seller_id int,
  store_id int,
  sale_date date,
  sale_amt bigint
)
distributed by hash(record_id)
properties("replication_num" = "1");


insert into sales_records values(1,2,3,'2020-02-02',10);

# 需要在Linux命令行执行
# mysql -h hadoop162 -uroot -P 9030 -paaaaaa
create materialized view store_amt as
select
store_id,
sum(sale_amt)
from sales_records
group by store_id;


SHOW ALTER TABLE MATERIALIZED VIEW FROM test_db;

desc sales_records all;

EXPLAIN SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
EXPLAIN SELECT seller_id, sum(sale_amt) FROM sales_records GROUP BY seller_id;
EXPLAIN SELECT sum(sale_amt) FROM sales_records;


create table advertiser_view_record(
time date,
advertiser varchar(10),
channel varchar(10),
user_id int
)
distributed by hash(time)
properties("replication_num" = "1");

insert into advertiser_view_record values('2020-02-02','a','app',123);

create materialized view advertiser_uv as
select
advertiser,
channel,
bitmap_union(to_bitmap(user_id))
from advertiser_view_record
group by advertiser, channel;

explain SELECT advertiser, channel, count(distinct user_id) FROM  advertiser_view_record GROUP BY advertiser, channel;

explain select record_id,seller_id,store_id from sales_records where store_id=3;

create materialized view mv_1 as
select
  store_id,
  record_id,
  seller_id,
  sale_date,
  sale_amt
from sales_records;

desc sales_records all;

explain select record_id,seller_id,store_id from sales_records where store_id=3;

use test_db;
CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");

insert into table1 values
(1,1,'jim',2),
(2,1,'grace',2),
(3,2,'tom',2),
(4,3,'bush',3),
(5,3,'helen',3);


truncate table table1;



















