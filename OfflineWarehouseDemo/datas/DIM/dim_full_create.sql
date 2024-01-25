-- DIM 层全量快照表创建

-- 商品维度表
DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full
(
    -- sku info
    `id`                   STRING COMMENT 'sku_id',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    -- spu info
    `spu_id`               STRING COMMENT 'spu编号',
    `spu_name`             STRING COMMENT 'spu名称',
    -- c3
    `category3_id`         STRING COMMENT '三级分类id',
    `category3_name`       STRING COMMENT '三级分类名称',
    -- c2
    `category2_id`         STRING COMMENT '二级分类id',
    `category2_name`       STRING COMMENT '二级分类名称',
    -- c1
    `category1_id`         STRING COMMENT '一级分类id',
    `category1_name`       STRING COMMENT '一级分类名称',
    -- tm
    `tm_id`                STRING COMMENT '品牌id',
    `tm_name`              STRING COMMENT '品牌名称',
    -- attr
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name:STRING>> COMMENT '平台属性',
    -- sale attr
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name:STRING>> COMMENT '销售属性',
    -- sku info
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据导入（主维表+相关维表join生成）
with sku as(
    select
        id,
        sku_name,
        price,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        create_time,
        category3_id,
        tm_id
    from ods_sku_info_full where dt='2020-06-14'
), spu as (
    select
        id,
        spu_name
    from ods_spu_info_full where dt='2020-06-14'
), tm as (
    select
        id,
        tm_name
    from ods_base_trademark_full where dt='2020-06-14'
), c3 as (
    select
        id,
        name,
        category2_id
    from ods_base_category3_full where dt='2020-06-14'
), c2 as (
    select
        id,
        name,
        category1_id
    from ods_base_category2_full where dt='2020-06-14'
), c1 as (
    select
        id,
        name
    from ods_base_category3_full where dt='2020-06-14'
), sav as (
    -- ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name:STRING>> COMMENT '平台属性',
    select
        -- 此处一个商品会有多个平台属性，后续与sku商品信息表join的时候会出错，因此需要一个商品的多个平台属性合并
--         id,
--         attr_id,
--         value_id,
--         attr_name,
--         value_name,
        collect_list(named_struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name, 'value_name', value_name)) sku_attr_values,
        sku_id
    from ods_sku_attr_value_full where dt='2020-06-14'
                                 group by sku_id
), ssav as (
    select
        -- ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name:STRING>> COMMENT '销售属性',
        -- 此处一个商品会有多个销售属性，后续与sku商品信息表join的时候会出错，因此需要一个商品的多个销售属性合并
        sku_id,
--         sale_attr_id,
--         sale_attr_value_id,
--         sale_attr_name,
--         sale_attr_value_name,
        collect_list(named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id,'sale_attr_name', sale_attr_name, 'sale_attr_value_name', sale_attr_value_name )) sku_sale_attr_values
    from ods_sku_sale_attr_value_full where dt='2020-06-14' group by sku_id
)
insert overwrite table dim_sku_full partition (dt='2020-06-14')
select
    sku.id,
    price,
    sku_name,
    sku_desc,
    weight,
    is_sale,
    spu_id,
    spu_name,
    category3_id,
    c3.name, -- category3_name,
    category2_id,
    c2.name, -- category2_name,
    category1_id,
    c1.name, --category1_name,
    tm_id,
    tm_name,
    sku_attr_values,
    sku_sale_attr_values,
    create_time
from sku
    left join spu on sku.spu_id = spu.id
    left join tm on sku.tm_id = tm.id
    left join c3 on sku.category3_id = c3.id
    left join c2 on c3.category2_id = c2.id
    left join c1 on c2.category1_id = c1.id
    left join ssav on sku.id = ssav.sku_id
    left join sav on sku.id = sav.sku_id
;



-- 优惠券维度表
DROP TABLE IF EXISTS dim_coupon_full;
CREATE EXTERNAL TABLE dim_coupon_full(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type_code` STRING COMMENT '购物券类型编码',
    `coupon_type_name` STRING COMMENT '购物券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       STRING COMMENT '可以领取的开始日期',
    `end_time`         STRING COMMENT '可以领取的结束日期',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
-- 导入数据
with c1 as (
    select
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time,
        case coupon_type
            when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
            when '3202' then concat('满', condition_num, '件打', (1-benefit_discount)*10, '折')
            else concat('减', condition_amount, '元')
        end benefit_rule -- 沉淀出通用的维度属性
    from ods_coupon_info_full where dt='2020-06-14'
), dc1 as (
    select
        dic_code coupon_type_code,
        dic_name coupon_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='32'
), dc2 as (
    select
        dic_code range_type_code,
        dic_name range_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='33'
)
insert overwrite table dim_coupon_full partition (dt='2020-06-14')
select
    id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    benefit_rule,
    create_time,
    range_type_code,
    range_type_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from c1 left join dc1 on c1.coupon_type = dc1.coupon_type_code
left join dc2 on c1.range_type = dc2.range_type_code
;


-- 活动维度表
--      主维表：活动规则表，相关维表：活动信息表
DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`        STRING COMMENT '活动ID',
    `activity_name`      STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`      STRING COMMENT '活动描述',
    `start_time`         STRING COMMENT '开始时间',
    `end_time`           STRING COMMENT '结束时间',
    `create_time`        STRING COMMENT '创建时间',
    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`      BIGINT COMMENT '满减件数',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`       STRING COMMENT '优惠规则',  -- 沉淀出通用的维度属性
    `benefit_level`      STRING COMMENT '优惠级别'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 导入数据
with atr as (
    select
        id activity_rule_id,
        activity_id,
        activity_type activity_type_code,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level,
        -- 沉淀出通用的维度属性
        case activity_type
            when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
            when '3102' then concat('满', condition_num, '件打', (1-benefit_discount)*10, '折')
            else concat('打', (1-benefit_discount)*10, '折')
        end benefit_rule
    from ods_activity_rule_full where dt='2020-06-14'
), at as (
    select
        id,
        activity_name,
        -- activity_type, -- 重复字段
        activity_desc,
        start_time,
        end_time,
        create_time
    from ods_activity_info_full where dt='2020-06-14'
), dc as (
    select
        dic_code,
        dic_name activity_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='31'
)
insert overwrite table dim_activity_full partition (dt='2020-06-14')
select
    activity_rule_id,
    activity_id,
    activity_name,
    activity_type_code,
    activity_type_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    benefit_rule,
    benefit_level
from atr left join at on atr.activity_id=at.id
left join dc on atr.activity_type_code=dc.dic_code
;

-- msck repair table ods_activity_info_full; -- 修复元数据


-- 地区维度表
--  主维表：省份表
--  从维表：地区表
DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`            STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',
    `region_id`     STRING COMMENT '地区id',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据导入
with pn as (
    select
        id,
        name province_name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province_full where dt='2020-06-14'
),rn as (
    select
        id, region_name
    from ods_base_region_full where dt='2020-06-14'
)
insert overwrite table dim_province_full partition(dt='2020-06-14')
select
    pn.id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    region_id,
    region_name
from pn left join rn on pn.region_id=rn.id
;


-- 日期维度表（业务系统没有，运营手工插入，上一年插入，类似交易日历）
DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`       STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
drop table if exists dim_date_tmp;
CREATE EXTERNAL TABLE dim_date_tmp
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`       STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    row format delimited fields terminated by '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/tmp/dim_date/'
;
load data inpath '/dateinfo' overwrite into table dim_date_tmp;
-- msck repair table dim_date_tmp;

insert overwrite table dim_date select * from dim_date_tmp;

show create table dim_date;


-- 用户维度表（增量拉链表）
--  主维表：用户信息表
--  相关维表：用户地址表（本次没有使用，如果要使用可以加，注意多值属性问题，用复杂数据类型包装）
DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 增量导入
--- 首日：导入表中国所有数据
---     首日 2020-06-14，是导入MySQL当日表中所有数据，此时表中所有数据都是当天最新数据，所以放入维度表9999-12-31分区中
insert overwrite table dim_user_zip partition (dt='9999-12-31')
select
    data.id,
    data.login_name,
    data.nick_name,
    data.name,
    data.phone_num,
    data.email,
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    -- 如果修改过，则生效日期为最后修改日期，否则为创建时间
    date_format(nvl(data.operate_time, data.create_time), 'yyyy-MM-dd'), -- date.start_date,
    "9999-12-31" -- date.end_date
from ods_user_info_inc where dt='2020-06-14' and type='bootstrap-insert';  -- type为bootstrap-start和bootstrap-complete之间的数据才是增量表当日数据

--- 每日加载(以2020-06-15为例)：导入当天新增和修改的数据
---   数据来源：前一日为止最新数据是在9999-12-31分区，当日最新数据在ODS层当日分区中
---   数据去处：当天为止最新数据放入9999-12-31分区中，失效数据放入当日-1时间分区中
set hive.exec.dynamic.partition.mode=nonstrict; -- 若要使用动态分区，需要设置hive参数为非严格模式
-- 第一种每日加载方案
with old as (
    select
        id ,
        login_name ,
        nick_name ,
        name ,
        phone_num ,
        email ,
        user_level ,
        birthday ,
        gender ,
        create_time ,
        operate_time ,
        start_date ,
        end_date
    from dim_user_zip where dt='9999-12-31'
), new as (
    select
        id ,
        login_name,
        nick_name,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date
    from(
        select
            data.id ,
            data.login_name ,
            data.nick_name ,
            data.name ,
            data.phone_num ,
            data.email ,
            data.user_level ,
            data.birthday ,
            data.gender ,
            data.create_time ,
            data.operate_time ,
            '2020-06-15' start_date,  -- 新数据的生效时间，直接是当天
            '9999-12-31' end_date, -- 新数据的失效时间，是9999-12-31
            row_number() over (partition by data.id order by ts desc) rn
        from ods_user_info_inc where dt='2020-06-15'
    ) t1 where rn = 1
), full_user as (
    select
        old.id old_id,
        old.login_name old_login_name,
        old.nick_name old_nick_name,
        old.name old_name,
        old.phone_num old_phone_num,
        old.email old_email,
        old.user_level old_user_level,
        old.birthday old_birthday,
        old.gender old_gender,
        old.create_time old_create_time,
        old.operate_time old_operate_time,
        old.start_date old_start_date,
        old.end_date old_end_date,
        new.id new_id ,
        new.login_name new_login_name ,
        new.nick_name new_nick_name ,
        new.name new_name ,
        new.phone_num new_phone_num ,
        new.email new_email ,
        new.user_level new_user_level ,
        new.birthday new_birthday ,
        new.gender new_gender ,
        new.create_time new_create_time ,
        new.operate_time new_operate_time,
        new.start_date new_start_date,
        new.end_date new_end_date
    from new full outer join  old on new.id = old.id
)
insert overwrite table dim_user_zip partition (dt)
-- 下面筛选最新数据
select
    `if`(new_id is not null, new_id, old_id),
    `if`(new_id is not null, new_login_name, old_login_name),
    `if`(new_id is not null, new_nick_name, old_nick_name),
    `if`(new_id is not null, new_name, old_name),
    `if`(new_id is not null, new_phone_num, old_phone_num),
    `if`(new_id is not null, new_email, old_email),
    `if`(new_id is not null, new_user_level, old_user_level),
    `if`(new_id is not null, new_birthday, old_birthday),
    `if`(new_id is not null, new_gender, old_gender),
    `if`(new_id is not null, new_create_time, old_create_time),
    `if`(new_id is not null, new_operate_time, old_operate_time),
    `if`(new_id is not null, new_start_date, old_start_date),
    `if`(new_id is not null, new_end_date, old_end_date),
    `if`(new_id is not null, new_end_date, old_end_date)  -- 动态分区字段
from full_user
where new_id is not null or ( old_id is not null and new_id is null )  -- 新数据id不为null 或者 旧数据id不为null&新数据id为null
union
-- 下面筛选失效数据
select
      old_id,
     old_login_name,
    old_nick_name,
     old_name,
    old_phone_num,
     old_email,
    old_user_level,
     old_birthday,
    old_gender,
     old_create_time,
     old_operate_time,
     old_start_date,
      cast(date_sub('2020-06-15', 1) as string),  -- 失效日期，当前日期-1天
      cast(date_sub('2020-06-15', 1) as string) -- 动态分区字段
from full_user where old_id is not null and new_id is not null;
;

-- 第二种每日加载方案：最新数据 unio 9999-12-31分区数据，按照用户分区，生效时间降序，每个用户排在第一位就是最新数据，排在第1位之后是失效数据
with old as (
    select
        id ,
        login_name ,
        nick_name ,
        name ,
        phone_num ,
        email ,
        user_level ,
        birthday ,
        gender ,
        create_time ,
        operate_time ,
        start_date ,
        end_date
    from dim_user_zip where dt='9999-12-31'
), new as (
    select
        id ,
        login_name,
        nick_name,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date
    from(
        select
            data.id ,
            data.login_name ,
            data.nick_name ,
            data.name ,
            data.phone_num ,
            data.email ,
            data.user_level ,
            data.birthday ,
            data.gender ,
            data.create_time ,
            data.operate_time ,
            '2020-06-15' start_date,  -- 新数据的生效时间，直接是当天
            '9999-12-31' end_date, -- 新数据的失效时间，是9999-12-31
            row_number() over (partition by data.id order by ts desc) rn
        from ods_user_info_inc where dt='2020-06-15'
    ) t1 where rn = 1
), full_user as (
    select
        id ,
        login_name,
        nick_name,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date
    from new
    union
    select
        id ,
        login_name,
        nick_name,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date
    from old
)
insert overwrite table dim_user_zip partition (dt)
select
    id ,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    if(rn=1,'9999-12-31', cast(date_sub('2020-06-15', 1) as string)),
    if(rn=1,'9999-12-31', cast(date_sub('2020-06-15', 1) as string))
from
(
    select
        id ,
        login_name,
        nick_name,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date,
        row_number() over (partition by id order by start_date desc ) rn
    from full_user
) t1
;

-- 正则表达式
-- desc function extended regexp;
-- 正则表达式语法：
-- . 代表任意字符
-- * 代表零次或多次
-- + 代表一次或多次
-- ? 代表0次或一次
-- {n} 代表n次
-- [xyz] 代表匹配其中一个字符
-- [a-zA-Z0-9] 代表匹配a-z或者A-Z或者0-9其中一个字符
-- \d 代表匹配任意一个数字
-- | 代表或者
-- ^ 代表字符串开头
-- $ 代表字符串的结尾

-- 正则表达式匹配手机号码
select '13767204544' regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\\d{8}$';
select '12767204544' regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\\d{8}$';

-- 正则表达式匹配邮箱格式
select '1398651518@qq.com' regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\\.[a-zA-Z0-9_-]+)+$';
