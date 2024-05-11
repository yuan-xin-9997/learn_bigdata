#!/bin/bash
# ODS层->DIM层 首日数据装载脚本
# ods_to_dim_init.sh all/表名
# todo 首日加载 在 全量快照表/拉链表 的区别

# 1. 判断参数是否传入
if [ $# -lt 1 ]
then
    echo "必须至少传入一个表名/all..."
    exit
fi

# 2. 判断日期是否传入，如果传入日期则加载指定日期的数据，如果没有传入则加载前一天的数据
# if [ "$2" == "" ];then
# #     datestr`date -d '-1 day' +%F`
#     datestr=$(date -d '-1 day' +%F)
# else
#     datestr=$2
# fi
# 也可以换成这种写法
# [ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

# 首日日期定死
datestr="2020-06-14"

# 商品维度sql数据加载语句
dim_sku_full_sql="
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
    from ods_sku_info_full where dt='$datestr'
), spu as (
    select
        id,
        spu_name
    from ods_spu_info_full where dt='$datestr'
), tm as (
    select
        id,
        tm_name
    from ods_base_trademark_full where dt='$datestr'
), c3 as (
    select
        id,
        name,
        category2_id
    from ods_base_category3_full where dt='$datestr'
), c2 as (
    select
        id,
        name,
        category1_id
    from ods_base_category2_full where dt='$datestr'
), c1 as (
    select
        id,
        name
    from ods_base_category3_full where dt='$datestr'
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
    from ods_sku_attr_value_full where dt='$datestr'
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
    from ods_sku_sale_attr_value_full where dt='$datestr' group by sku_id
)
insert overwrite table dim_sku_full partition (dt='$datestr')
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
"

# 优惠券维度表sql数据加载
dim_coupon_full_sql="
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
    from ods_coupon_info_full where dt='$datestr'
), dc1 as (
    select
        dic_code coupon_type_code,
        dic_name coupon_type_name
    from ods_base_dic_full where dt='$datestr' and parent_code='32'
), dc2 as (
    select
        dic_code range_type_code,
        dic_name range_type_name
    from ods_base_dic_full where dt='$datestr' and parent_code='33'
)
insert overwrite table dim_coupon_full partition (dt='$datestr')
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
"

# 活动维度表数据加载sql
dim_activity_full_sql="
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
    from ods_activity_rule_full where dt='$datestr'
), at as (
    select
        id,
        activity_name,
        -- activity_type, -- 重复字段
        activity_desc,
        start_time,
        end_time,
        create_time
    from ods_activity_info_full where dt='$datestr'
), dc as (
    select
        dic_code,
        dic_name activity_type_name
    from ods_base_dic_full where dt='$datestr' and parent_code='31'
)
insert overwrite table dim_activity_full partition (dt='$datestr')
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
"

# 地区维度表数据加载sql
dim_province_full_sql="
with pn as (
    select
        id,
        name province_name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province_full where dt='$datestr'
),rn as (
    select
        id, region_name
    from ods_base_region_full where dt='$datestr'
)
insert overwrite table dim_province_full partition(dt='$datestr')
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
"

# 用户维度表首日数据加载sql
dim_user_zip_sql="
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
from ods_user_info_inc where dt='$datestr' and type='bootstrap-insert';
"


# 3. 根据第一个参数匹配加载
case $1 in
"all")
    /opt/module/hive/bin/hive -e "use gmall; $dim_activity_full_sql; $dim_coupon_full_sql; $dim_province_full_sql; $dim_sku_full_sql; $dim_user_zip_sql"

;;
"dim_activity_full")
    /opt/module/hive/bin/hive -e "use gmall; $dim_activity_full_sql"
;;
"dim_coupon_full")
    /opt/module/hive/bin/hive -e "use gmall; $dim_coupon_full_sql"
;;
"dim_province_full")
    /opt/module/hive/bin/hive -e "use gmall; $dim_province_full_sql"
;;
"dim_sku_full")
    /opt/module/hive/bin/hive -e "use gmall; $dim_sku_full_sql"
;;
"dim_user_zip")
    /opt/module/hive/bin/hive -e "use gmall; $dim_user_zip_sql"
;;
*)
    echo "参数传入错误"
;;
esac
