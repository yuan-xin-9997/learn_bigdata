/*
第5章 Spark Core实战
需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
鞋			点击数 下单数  支付数
衣服		点击数 下单数  支付数
电脑		点击数 下单数  支付数
例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
todo 需求：Top10热门品类
    以下为方案二实现
            可以实现需求，且只有一次group by 一次order by
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

-- 对order_category_ids进行explode炸裂
select
    click_category_id,
    order_category_id,
    pay_category_ids
from user_visitor
lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
where search_keyword='null';

-- 基于上述炸裂的基础，继续对pay_category_ids炸裂
select
    click_category_id,
        order_category_id,
        pay_category_id
from (
    select
        click_category_id,
        order_category_id,
        pay_category_ids
    from user_visitor
    lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
    where search_keyword='null'
     ) t1 lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id;

-- 使用case when将三列id合并为一列
select
    case
        when click_category_id!='-1' then click_category_id
        when order_category_id!='null' then order_category_id
        else pay_category_id
    end id
from (
    select
        click_category_id,
        order_category_id,
        pay_category_ids
    from user_visitor
    lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
    where search_keyword='null'
     ) t1 lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id;

-- 使用if函数增加表示该id是属于点击、下单、还是支付
select
    case
        when click_category_id!='-1' then click_category_id
        when order_category_id!='null' then order_category_id
        else pay_category_id
    end id,
    if(click_category_id!=-1, 1, 0) click_flag,
    if(order_category_id!='null', 1, 0) order_flag,
    if(pay_category_id!='null', 1, 0) pay_flag
from (
    select
        click_category_id,
        order_category_id,
        pay_category_ids
    from user_visitor
    lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
    where search_keyword='null'
     ) t1 lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id;

-- 对ID字段进行分组
select
    id,
    sum(click_flag) click_num,
    sum(order_flag) order_num,
    sum(pay_flag) pay_num
from (
        select
            case
                when click_category_id!='-1' then click_category_id
                when order_category_id!='null' then order_category_id
                else pay_category_id
            end id,
            if(click_category_id!=-1, 1, 0) click_flag,
            if(order_category_id!='null', 1, 0) order_flag,
            if(pay_category_id!='null', 1, 0) pay_flag
        from (
            select
                click_category_id,
                order_category_id,
                pay_category_ids
            from user_visitor
            lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
            where search_keyword='null'
             ) t1 lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id
     ) t2 group by id;

-- 按点击、下单、支付排序，并取前10
select
    id,
    click_num,
    order_num,
    pay_num
from (
    select
        id,
        sum(click_flag) click_num,
        sum(order_flag) order_num,
        sum(pay_flag) pay_num
    from (
            select
                case
                    when click_category_id!='-1' then click_category_id
                    when order_category_id!='null' then order_category_id
                    else pay_category_id
                end id,
                if(click_category_id!=-1, 1, 0) click_flag,
                if(order_category_id!='null', 1, 0) order_flag,
                if(pay_category_id!='null', 1, 0) pay_flag
            from (
                select
                    click_category_id,
                    order_category_id,
                    pay_category_ids
                from user_visitor
                lateral view explode(split(order_category_ids, ',')) tmp as order_category_id
                where search_keyword='null'
                 ) t1 lateral view explode(split(pay_category_ids, ',')) tmp as pay_category_id
         ) t2 group by id
     ) t3 order by click_num desc ,order_num desc, pay_num desc
limit 10;

-- todo 上述SQL可以实现需求，且只有一次group by 一次order by