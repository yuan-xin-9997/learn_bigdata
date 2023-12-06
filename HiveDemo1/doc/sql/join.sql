-- 内连接（等值连接）
select
    e.deptno,
    d.deptno,
    e.ename,
    d.dname
from emp e
join dept d
on e.deptno = d.deptno;
select
    e.deptno,
    d.deptno,
    e.ename,
    d.dname
from emp e
inner join dept d
on e.deptno = d.deptno;

-- 左外连接
select
           e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
left join dept d on e.deptno=d.deptno;

-- 如果只想保留左表特有数据，应该如何实现？
select
           e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
left join dept d on e.deptno=d.deptno
where d.deptno is null;

-- 右外连接
select
    e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
right join dept d
on e.deptno=d.deptno;

-- 如果只想保留右表特有数据，应该如何实现？
select
    e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
right join dept d
on e.deptno=d.deptno
where e.deptno is null;

-- 全连接（满外连接）
select
           e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
full join dept d
on e.deptno = d.deptno;


-- 如果只想保留两个表各自特有数据，应该如何实现？
select
           e.empno,
           e.ename,
           e.deptno,
           d.deptno,
           d.dname
from emp e
full join dept d
on e.deptno = d.deptno
where e.deptno is null or d.deptno is null;

-- 多表连接
select
    e.ename,
    d.dname,
    l.loc_name
from emp e
inner join dept d on e.deptno=d.deptno
inner join location l on d.loc=l.loc;


-- 笛卡尔积
-- 第一种情况：连接条件省略
select
    e.deptno,
    d.deptno,
    e.ename,
    d.dname
from emp e
join dept d;
-- 第二种情况：连接条件无效
select
    e.deptno,
    d.deptno,
    e.ename,
    d.dname
from emp e
join dept d
on 1=1;
--第三种情况：从两张表中查询数据
select
    e.deptno,
    d.deptno,
    e.ename,
    d.dname
from emp e, dept d;