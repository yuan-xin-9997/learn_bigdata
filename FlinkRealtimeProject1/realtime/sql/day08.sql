
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

