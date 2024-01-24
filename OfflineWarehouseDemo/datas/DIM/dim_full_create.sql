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

--- 每日：导入当天新增和修改的数据