-- 9.4.5 主流文件存储格式对比实验
-- 从存储文件的压缩比和查询速度两个角度对比。
-- 存储文件的压缩比测试：

-- TextFile
-- 1）创建表log_text，设置其存储数据格式为TEXTFILE
create table log_text (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as textfile;

load data local inpath '/opt/module/hive/datas/log.data' into table log_text ;


-- ORC
-- 1）创建表log_orc，存储数据格式为ORC
create table log_orc(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="NONE"); // 由于ORC格式时自带压缩的，这设置orc存储不使用压缩

-- 不可以通过load导入数据
insert into table log_orc select * from log_text ;

dfs -du -h /user/hive/warehouse/log_orc/ ;


-- Parquet
-- 1）创建表log_parquet，设置其存储数据格式为parquet

create table log_parquet(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as parquet ;

insert into table log_parquet select * from log_text ;

dfs -du -h /user/hive/warehouse/log_parquet/ ;



-- 6．存储文件的查询速度测试：

insert overwrite local directory '/opt/module/hive/datas/log_text' select substring(url,1,4) from log_text ;
-- Time taken: 8.8 seconds

insert overwrite local directory '/opt/module/hive/datas/log_orc' select substring(url,1,4) from log_orc ;
--Time taken: 7.545 seconds

insert overwrite local directory '/opt/module/hive/data/slog_parquet' select substring(url,1,4) from log_parquet ;
-- Time taken: 8.568 seconds


-- #####################################################################################################


-- 创建一个ZLIB压缩的ORC存储方式
-- （1）创建表log_orc_zlib表，设置其使用ORC文件格式，并使用ZLIB压缩
create table log_orc_zlib(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="ZLIB");

insert into log_orc_zlib select * from log_text;

dfs -du -h /user/hive/warehouse/log_orc_zlib/ ;

-- 创建一个SNAPPY压缩的ORC存储方式
-- （1）创建表log_orc_snappy表，设置其使用ORC文件格式，并使用snappy压缩
create table log_orc_snappy(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="SNAPPY");

insert into log_orc_snappy select * from log_text;
dfs -du -h /user/hive/warehouse/log_orc_snappy/ ;

-- 创建一个SNAPPY压缩的parquet存储方式
-- （1）创建表log_parquet_snappy，设置其使用Parquet文件格式，并使用SNAPPY压缩
create table log_parquet_snappy(
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string
)
row format delimited fields terminated by '\t'
stored as parquet
tblproperties("parquet.compression"="SNAPPY");
insert into log_parquet_snappy select * from log_text;
dfs -du -h /user/hive/warehouse/log_parquet_snappy/ ;
