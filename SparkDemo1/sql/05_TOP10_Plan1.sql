/*
第5章 Spark Core实战
需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
鞋			点击数 下单数  支付数
衣服		点击数 下单数  支付数
电脑		点击数 下单数  支付数
例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
todo 需求：Top10热门品类
    以下为方案一实现
            SQL可以实现功能，但是非常蠢，shuffle次数非常多，耗时长，3次group by、2次 full join，order by 都会有shuffle
*/


create table user_visitor(
    `date` string,
    user_id string,
    session_id string,
    page_id string,
    action_time string,
    search_keyword string,
    click_category_id string,
    click_product_id string,
    order_category_ids string,
    order_product_ids string,
    pay_category_ids string,
    pay_product_ids string,
    city_id string
)
row format delimited fields terminated by "_";

select *
from user_visitor;


-- todo 获取最热门的10个品类

-- 热门品类标准：品类的点击数、下单数、支付数排序取前10

-- 1.1 统计每个品类的点击数
select
    click_category_id,
    count(1)  click_num
from user_visitor
-- 仅筛选点击行为
where click_category_id!='-1' and search_keyword='null'
group by click_category_id;

-- 1.2 统计每个品类的下单数
select
    --order_category_ids,
    order_category_id,
    count(1)  order_num
from user_visitor
lateral view explode(split(order_category_ids, ',')) tmp as order_category_id  -- 对列进行炸开
where search_keyword='null' and order_category_ids!='null'
group by order_category_id;

-- 1.3 统计每个品类的支付数
select
    --order_category_ids,
    pay_category_id,
    count(1)  pay_num
from user_visitor
lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id  -- 对列进行炸开
where search_keyword='null' and pay_category_ids!='null'
group by pay_category_id;


-- 2 对每个品类点击数、下单数、支付数进行full outer join，得到每个品类的相关数据
-- todo 空字段赋值-NVL（防止空字段参与计算），
--      给值为NULL的数据赋值，它的格式是NVL( value，default_value)
--      如果value为NULL，则NVL函数返回default_value的值，否则返回value的值
--      如果两个参数都为NULL ，则返回NULL。
select
    --click_category_id,
    nvl(cc.click_category_id, nvl(oc.order_category_id, nvl(pc.pay_category_id, null ))) id,
    click_num,
    order_num,
    pay_num
from
    (
        select
            click_category_id,
            count(1)  click_num
        from user_visitor
        -- 仅筛选点击行为
        where click_category_id!='-1' and search_keyword='null'
        group by click_category_id
    ) cc full outer join (
        select
            --order_category_ids,
            order_category_id,
            count(1)  order_num
        from user_visitor
        lateral view explode(split(order_category_ids, ',')) tmp as order_category_id  -- 对列进行炸开
        where search_keyword='null' and order_category_ids!='null'
        group by order_category_id
    ) oc on cc.click_category_id = oc.order_category_id full outer join (
        select
            --order_category_ids,
            pay_category_id,
            count(1)  pay_num
        from user_visitor
        lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id  -- 对列进行炸开
        where search_keyword='null' and pay_category_ids!='null'
        group by pay_category_id
    ) pc on nvl(oc.order_category_id, cc.click_category_id) = pc.pay_category_id;


-- 3. 排序
select
    --click_category_id,
    nvl(cc.click_category_id, nvl(oc.order_category_id, nvl(pc.pay_category_id, null ))) id,
    click_num,
    order_num,
    pay_num
from
    (
        select
            click_category_id,
            count(1)  click_num
        from user_visitor
        -- 仅筛选点击行为
        where click_category_id!='-1' and search_keyword='null'
        group by click_category_id
    ) cc full outer join (
        select
            --order_category_ids,
            order_category_id,
            count(1)  order_num
        from user_visitor
        lateral view explode(split(order_category_ids, ',')) tmp as order_category_id  -- 对列进行炸开
        where search_keyword='null' and order_category_ids!='null'
        group by order_category_id
    ) oc on cc.click_category_id = oc.order_category_id full outer join (
        select
            --order_category_ids,
            pay_category_id,
            count(1)  pay_num
        from user_visitor
        lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id  -- 对列进行炸开
        where search_keyword='null' and pay_category_ids!='null'
        group by pay_category_id
    ) pc on nvl(oc.order_category_id, cc.click_category_id) = pc.pay_category_id
order by click_num desc ,order_num desc, pay_num desc
limit 10
;

-- todo 评价，上述SQL可以实现功能，但是非常蠢，shuffle次数非常多，耗时长，3次group by、2次 full join，order by 都会有shuffle