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

--- 6.4 排序

--- 6.4.1 全局排序--> order by
----- 查询员工信息按照薪水的升序排列

----- 查询员工信息按照薪水的降序排列

----- 查询员工信息按照薪水的两倍升序排列

----- 查询员工表，按照部门和工资的升序排列



---- 6.4.2 分区排序的排序 --> Sort by
----- 注意要设置Reduce的个数

----- 根据部门编号降序查看员工信息

----- 查看每个部门，并且每个部门按照薪水降序排列能做到吗？


----- 6.4.3 分区排序的分区---> distrubute by
---- 按照部门编号分区，再按照员工薪水降序排列


----- 6.4.4 如果分区的字段和排序的字段一致，可简写为cluster by

---- 对emp表，按照deptno字段分区，按照deptno字段排序
--- 1

--- 2
