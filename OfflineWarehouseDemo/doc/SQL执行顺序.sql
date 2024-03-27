-- 1.查询语句语法：
-- SELECT [ALL | DISTINCT] select_expr, select_expr, ...
-- FROM table_reference
-- [WHERE where_condition]
-- [GROUP BY col_list]
-- [ORDER BY col_list]
-- [CLUSTER BY col_list| [DISTRIBUTE BY col_list] [SORT BY col_list]]
-- [LIMIT number]

-- 2.书写次序和执行次序
-- 顺序	书写次序	书写次序说明	执行次序	执行次序说明
-- 1	select	查询	from	先执行表与表直接的关系
-- 2	from	先执行表与表直接的关系	on
-- 3	join on		join
-- 4	where		where	过滤
-- 5	group by	分组	group by	分组
-- 6	having	分组后再过滤	having	分组后再过滤
-- 7	distribute by
--      cluster by	4个by	select	查询
-- 8	sort by		distinct	去重
-- 9	order by		distribute by
--      cluster by	4个by
-- 10	limit	限制输出的行数	sort by
-- 11	union/union all	合并	order by
-- 12			limit	限制输出的行数
-- 13			union /union all	合并


/*
 SQL运行顺序
 select
    xxx.  xxxx
 from A join B
 where xxx
 group by xxx
 having xxx

 在hive中，where中能在Join前运行的一定会先运行，即谓词下推。因为join会产生shuffle
 随后，运行join，再运行where中必须join后运行的过滤
 之后group by
 having
 最后是select
 */

explain
select recent_days,
       count(*) new_user_count
from dwd_user_register_inc -- 粒度：一行代表一个注册成功的用户
         lateral view explode(array(1, 7, 30)) tmp as recent_days -- 第2步执行：使用侧窗函数列转行，复制3份数据，并添加recent_days
where dt > date_sub('2020-06-14', 30)          -- 第1步执行：取最近30天所有用户注册的数据
  and dt > date_sub('2020-06-14', recent_days) -- 第3步：过滤出要统计的时间周期范围内的数据
group by recent_days -- 将过滤后的复制了3份的数据集按照recent_days分组
;


select date_sub('2020-06-14', 30);

/*
rawDataSize         ,374621
totalSize           ,14853
 */
desc formatted dwd_user_register_inc;

/*
 rawDataSize         ,57000
totalSize           ,1842
 */
desc formatted dwd_user_register_inc partition (dt='2020-06-15');


