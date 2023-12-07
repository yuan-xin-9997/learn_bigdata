-- 建表语句
--- dept
create external table if not exists dept(
    deptno int,
    dname string,
    loc int
)
row format delimited
    fields terminated by '\t';

--- emp
create external table if not exists emp(
   empno int,
   ename string,
   job string,
   mgr int,
   hiredate string,
   sal double,
   comm double,
   deptno int)
row format delimited fields terminated by '\t';

--- 6.3 Join

--- 6.3.1 等值join
---- 根据dept表和emp表中deptno字段相等，查询empno、deptno、ename、dname等信息

--- 6.3.2 表的别名
--- 合并emp表和dept表

--- 6.3.3 内连接
--- 只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。


--- 6.3.4 左外连接
--- JOIN操作符左边表中符合WHERE子句的所有记录将会被返回。

--- 如果只想保留左表特有数据，应该如何实现？


--- 6.3.5 右外连接
--- JOIN操作符右边表中符合WHERE子句的所有记录将会被返回。

--- 如果只想保留右表特有数据，应该如何实现？

--- 6.3.6 满外连接
--- 将会返回所有表中符合WHERE语句条件的所有记录。

--- 如果只想保留两个表各自特有数据，应该如何实现？


--- 6.3.7 多表连接


--- 6.3.8 笛卡尔积

