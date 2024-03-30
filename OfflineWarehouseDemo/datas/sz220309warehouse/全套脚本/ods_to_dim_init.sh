#!/bin/bash
if [ -n "$2" ]
then
	do_date=$2
else
	do_date= $(date -d yesterday +%F) 
fi

echo 当前要操作的日期是$do_date

dim_activity_full="
with
    activity_rule as (
        select
               id activity_rule_id,
               activity_id,
               condition_amount,
               condition_num,
               benefit_amount,
               benefit_discount,
               case activity_type
                   when '3101' then concat('满',condition_amount,'减',benefit_amount)
                   when '3102' then concat('满',condition_num,'打',10-benefit_discount*10,'折')
                   when '3103' then concat('立打',10-benefit_discount*10,'折')
               end benefit_rule,
               benefit_level
        from ods_activity_rule_full
        where dt='$do_date'

    ),
    activity_info as (

          select
                 id,
               activity_name,
               activity_type activity_type_code,
               activity_desc,
               start_time,
               end_time,
               create_time
            from ods_activity_info_full
            where dt='$do_date'
    ),
    dic_info as ( 
             select
                  dic_code,dic_name  activity_type_name
             from ods_base_dic_full
             where dt='$do_date' and parent_code='31'
         )
insert overwrite table dim_activity_full partition (dt='$do_date')
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
from activity_rule
left join activity_info on activity_rule.activity_id = activity_info.id
left join dic_info on activity_info.activity_type_code=dic_info.dic_code;
"
dim_coupon_full="
with coupon_info as (
    select id,
           coupon_name,
           coupon_type coupon_type_code,
           condition_amount,
           condition_num,
           activity_id,
           benefit_amount,
           benefit_discount,
           case coupon_type
               when '3201' then concat('满', condition_amount, '减', benefit_amount)
               when '3202' then concat('满', condition_num, '打', 10 - benefit_discount * 10, '折')
               when '3203' then concat('立减', benefit_amount, '元')
               end     benefit_rule,
           create_time,
           range_type  range_type_code,
           limit_num,
           taken_count,
           start_time,
           end_time,
           operate_time,
           expire_time
    from ods_coupon_info_full
    where dt = '$do_date'
),
     dic_info1 as (
              select
                   dic_code,dic_name  coupon_type_name
              from ods_base_dic_full
              where dt='$do_date' and parent_code='32'
          ),
     dic_info2 as (
              select
                   dic_code,dic_name range_type_name
              from ods_base_dic_full
              where dt='$do_date' and parent_code='33'
          )
insert overwrite table dim_coupon_full partition (dt='$do_date')
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
from coupon_info
left join dic_info1 on coupon_info.coupon_type_code = dic_info1.dic_code
left join dic_info2 on coupon_info.range_type_code = dic_info2.dic_code;
"
dim_date="
insert overwrite  table dim_date select * from tmp_dim_date_info;
"
dim_province_full="
with
    province_info as (
          select
                   id,
                   name province_name,
                   area_code,
                   iso_code,
                   iso_3166_2,
                   region_id
            from ods_base_province_full
            where dt='$do_date'
    ),
     region_info as (

           select
                   id,region_name
             from ods_base_region_full
             where dt='$do_date'
     )
insert overwrite table dim_province_full partition (dt='$do_date')
select
        province_info.id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       region_id,
       region_name
from province_info left join region_info on province_info.region_id = region_info.id;
"
dim_sku_full="
with
    sku_info as (
        -- 从主维度表中查询的数据
           select
                id,
               price,
               sku_name,
               sku_desc,
               weight,
               is_sale,
               spu_id,
                  create_time,
                  tm_id,category3_id
           -- 每个分区中，一个sku是一行
            from ods_sku_info_full
            where dt='$do_date'

    ),
    spu_info as (
        -- 从次维度表中查询的数据
            select
                id,
                spu_name
            from ods_spu_info_full
            where dt='$do_date'


    ),
     tm_info as (

           select
                id, tm_name
             from ods_base_trademark_full
             where dt='$do_date'

     ),
     c3_info as (

           select
                    id category3_id,
                  name category3_name,
                  category2_id
             from ods_base_category3_full
             where dt='$do_date'


     ),
     c2_info as (

          select
                    id category2_id,
                  name category2_name,
                  category1_id
             from ods_base_category2_full
             where dt='$do_date'
     ),
     c1_info as (

           select
                    id category1_id,
                  name category1_name
             from ods_base_category1_full
             where dt='$do_date'


     ),
     sku_attr_values_info as (
                -- 从次维度表中查询的结果

           select
                    sku_id,collect_list(named_struct('attr_id',attr_id,'value_id',
                        value_id,'attr_name',attr_name,'value_name',value_name)) sku_attr_values
             from ods_sku_attr_value_full
             where dt='$do_date'
            group by sku_id


     ),
     sku_sale_attr_values_info as (
                -- 从次维度表中查询的结果

           select
                    sku_id,collect_list(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',
                        sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sku_sale_attr_values
             from ods_sku_sale_attr_value_full
             where dt='$do_date'
            group by sku_id


     )
insert overwrite table dim_sku_full partition (dt='$do_date')
select
       sku_info.id,
       price,
       sku_name,
       sku_desc,
       weight,
       is_sale,
       spu_id,
       spu_name,
       sku_info.category3_id,
       category3_name,
       c2_info.category2_id,
       category2_name,
       c1_info.category1_id,
       category1_name,
       tm_id,
       tm_name,
       sku_attr_values,
       sku_sale_attr_values,
       create_time
       -- 处理的数据在ods层的dt是哪一天，dim层的dt是一致的
from
--主维度表
sku_info
    left join spu_info on sku_info.spu_id = spu_info.id
    left join tm_info on sku_info.tm_id = tm_info.id
    left join c3_info on sku_info.category3_id = c3_info.category3_id
    left join c2_info on c3_info.category2_id = c2_info.category2_id
    left join c1_info on c1_info.category1_id = c2_info.category1_id
    left join sku_attr_values_info on sku_info.id = sku_attr_values_info.sku_id
    left join sku_sale_attr_values_info on sku_info.id = sku_sale_attr_values_info.sku_id;
"
dim_user_zip="
-- user_info 在6-14日时，采集截止到6-14日mysql中 user_info表中的所有用户
-- 由于mysql只会保留最新的信息，因此截止到6-14日，当时采集的所有用户都是最新的状态。这些用户被采集到
-- ods_user_info_inc/dt='$do_date'分区中

insert overwrite table dim_user_zip partition (dt='9999-12-31')
select
       -- 从data struct<>取
       data.id,
       data.login_name,
       data.nick_name,
       md5(data.name),
       md5(if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',data.phone_num,null)),
       md5(if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',data.email,null)),
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       -- 额外生成
       -- 入库日期
       '$do_date' start_date,
       '9999-12-31' end_date
       -- 按照end_date进行分区
from ods_user_info_inc
-- 过滤出有数据的记录
where dt='$do_date' and type='bootstrap-insert';
"

case $1 in
"dim_activity_full")
	hive --database gmall -e "$dim_activity_full"
	;;
"dim_coupon_full")
	hive --database gmall -e "$dim_coupon_full"
	;;
"dim_date")
	hive --database gmall -e "$dim_date"
	;;
"dim_province_full")
	hive --database gmall -e "$dim_province_full"
	;;
"dim_sku_full")
	hive --database gmall -e "$dim_sku_full"
	;;
"dim_user_zip")
	hive --database gmall -e "$dim_user_zip"
	;;
"all")
	hive --database gmall -e "$dim_activity_full$dim_coupon_full$dim_date$dim_province_full$dim_sku_full$dim_user_zip"
	;;
esac

