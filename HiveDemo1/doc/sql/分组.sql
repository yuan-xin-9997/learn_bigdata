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


--- 6.2 分组
---- 6.2.1 Group by 语句
---- 计算emp表中每个部门的平均薪资

---- 计算emp表中每个部门中每个岗位的最高薪水

--- 6.2.2 Having 语句

---- 计算emp中每个部门的平均薪资

---- 计算每个部门的平均薪水大于2000的部门
