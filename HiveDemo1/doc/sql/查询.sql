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


-- 6.1 基本查询
--- 6.1.1 全表和特定列查询
---- 全表查询emp



--- 查询emp表中员工姓名，主管，薪水，奖金



--- 6.1.2 列的别名
----- 查询员工名字和部门编号 并赋予别名





--- 6.1.3 算数运算符
----- 查询每个员工的总收入

---- 查询员工薪水税后多少


--- 6.1.4 常用函数
--- 求总行数count()

--- 求工资最大值max()

--- 求工资的总和sum()

--- 求工资的平均值avg()


--- 6.1.5 limit 语句

--- 查询emp表中员工姓名，薪水的五条数据

--- 查询emp表中第3、4、5条数据


--- 6.1.6 where 语句
--- 查询出薪水大于1000的所有员工

--- 查询述总收入大于1000的所有员工


--- 6.1.7 比较运算符
----- 查询出薪水等于5000的所有员工

----- 查询工资在500和1000之间的员工

----- 查询奖金comm为空的员工姓名等信息

----- 查询工资是1500或5000的员工信息


--- 6.1.8 Like 和 RLike
---- 查找名字以A开头的员工信息--Like

--- 查找名字第二个字母为A的员工信息--Like

---- 查找名字中带有A的员工信息---Like

---- 查找名字以A开头的员工信息--RLike

---- 查找名字中带有A的员工信息---RLike


--- 6.1.9 逻辑运算符（AND、or、not)
---- 查询薪水大于1000，并且部门编号是30的员工信息

---- 查询薪水大于1000， 或者部门编号是30的员工信息

---- 查询除了20号部门和30号部门以外的员工信息
