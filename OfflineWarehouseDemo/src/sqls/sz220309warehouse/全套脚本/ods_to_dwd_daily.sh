#!/bin/bash
if [ -n "$2" ]
then
	do_date=$2
else
	#echo 必须给我传一个日期
	#exit
	do_date= $(date -d yesterday +%F)
fi

echo 当前要操作的日期是$do_date


dwd_interaction_favor_add_inc="
insert overwrite table dwd_interaction_favor_add_inc partition (dt='$do_date')
-- 首日采集的是 favor_info表的最终状态，那么每一条记录都是insert到表中的，所以每一条记录都有过一次收藏的事实
  select
       data.id,
       data.user_id,
       data.sku_id,
       '$do_date' date_id,
       data.create_time
    from ods_favor_info_inc
    where dt='$do_date' and type='insert';
"


dwd_tool_coupon_used_inc="
insert overwrite table dwd_tool_coupon_used_inc partition (dt='$do_date')
  select
       data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       '$do_date' date_id,
       data.used_time payment_time
    from ods_coupon_use_inc
    where dt='$do_date'  and type='update'
     -- 过滤出支付的用券事实
   and old['coupon_status'] = '1402' and data.coupon_status = '1403';
"

dwd_trade_cart_add_inc="
with
    cart_add_info as ( 
        select
            data.id,
            data.user_id,
            data.sku_id,
            data.create_time,
            data.source_id,
            data.source_type source_type_code,
               -- 向购物车中添加了多少件商品
            \\`if\\`( type='insert',data.sku_num,data.sku_num - old['sku_num']) sku_num
        from ods_cart_info_inc
        where dt='$do_date' and
              (
                type='insert'
                    or
               -- 修改后的sku_num > 修改前的sku_num
               -- 还需不需要再添加一个  old['sku_num'] 不为null的判断?
               -- 不需要
               (type='update' and data.sku_num - old['sku_num'] > 0)
              )
        
    ),
    dic_info as ( 
             select
                  dic_code,dic_name source_type_name
             from ods_base_dic_full
             where dt='$do_date' and parent_code='24'
         )
insert overwrite table dwd_trade_cart_add_inc partition (dt='$do_date')
select
        id,
        user_id,
        sku_id,
       '$do_date' date_id,
        create_time,
        source_id,
        source_type_code,
        source_type_name,
        sku_num
from cart_add_info left join dic_info
on cart_add_info.source_type_code = dic_info.dic_code;
"
dwd_trade_cart_full="
 insert overwrite table dwd_trade_cart_full partition (dt='$do_date')
  select
       id,
       user_id,
       sku_id,
       sku_name,
       sku_num
    from ods_cart_info_full
    where dt='$do_date'
    and is_ordered = 0;
"
dwd_trade_order_detail_inc="
with
    order_detail as ( 
        
          select
                   data.id,
                   data.order_id,
                   data.sku_id,
                   data.create_time,
                   data.source_id,
                   data.source_type source_type_code,
                   data.sku_num,
                 -- 原始金额 = 单价 * 数量
                   data.order_price * data.sku_num split_original_amount,
                   data.split_activity_amount,
                   data.split_coupon_amount,
                   data.split_total_amount
            from ods_order_detail_inc
            where dt='$do_date' and type='insert'
        
    ),
     dic_info as (
              select
                   dic_code,dic_name   source_type_name
              from ods_base_dic_full
              where dt='$do_date' and parent_code='24'
          ),
     order_info as (

           select
                data.id,
                data.user_id,
                data.province_id
             from ods_order_info_inc
             where dt='$do_date' and type='insert'

     ),
     order_detail_coupon as (

           select
                   data.order_detail_id,
                   data.coupon_id
             from ods_order_detail_coupon_inc
             where dt='$do_date' and type='insert'

     ),
     order_detail_activity as (

           select
                   data.order_detail_id,
                   data.activity_id,
                   data.activity_rule_id
             from ods_order_detail_activity_inc
             where dt='$do_date' and type='insert'

     )
insert overwrite table dwd_trade_order_detail_inc partition (dt='$do_date')
select
       order_detail.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       '$do_date' date_id,
       create_time,
       source_id,
       source_type_code,
       source_type_name,
       sku_num,
       split_original_amount,
       -- LongColumnVector cannot be cast to DecimalColumnVector
       -- 字段是整数，nvl(xxx,0)，如果是金额是小数，补nvl(xxx,0.0)
       nvl(split_activity_amount,0.0),
       nvl(split_coupon_amount,0.0),
       split_total_amount
from order_detail
left join order_info on order_detail.order_id = order_info.id
left join order_detail_coupon on order_detail.id = order_detail_coupon.order_detail_id
left join order_detail_activity on order_detail.id = order_detail_activity.order_detail_id
left join dic_info on dic_info.dic_code = order_detail.source_type_code;
"

dwd_trade_pay_detail_suc_inc="
    with
        payment_info as (
            -- 截止到6-15日所有支付成功的事实
            -- 基于payment_info表只允许更新和交易相关的字段，例如callback_time等
              select
                    data.id,
                    data.callback_time,
                    data.payment_type payment_type_code,
                     data.order_id
                from ods_payment_info_inc
                where dt='$do_date' and type='update'
                  --只要支付成功的
                    and isnotnull(data.callback_time)

        ),dic_info as ( 
                 select
                      dic_code,dic_name    payment_type_name
                 from ods_base_dic_full
                 where dt='$do_date' and parent_code='11'
             ),
         order_detail as (
                -- 跨天支付问题:  6-15日支付的订单可能是6-14日下的也可能是6-15日下的
               select
                       *
                 from dwd_trade_order_detail_inc
               -- 截止到6-14日所有的订单
                 where dt = '$do_date' or dt=date_sub('$do_date',1)

         )
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt='$do_date')
select payment_info.id,
       payment_info.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type_code,
       payment_type_name,
       '$do_date' date_id,
       callback_time,
       source_id,
       source_type_code,
       source_type_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount split_payment_amount
from payment_info
    -- 只要支付成功的订单详情
left join order_detail on payment_info.order_id = order_detail.order_id
left join dic_info on dic_info.dic_code = payment_info.payment_type_code;
"


dwd_traffic_page_view_inc="
with
      log_info as (
        select
               common.ar,
               common.ba brand,
               common.ch channel,
               common.is_new,
               common.md model,
               common.mid mid_id,
               common.os operate_system,
               common.uid user_id,
               common.vc version_code,
                  -- page
                   page.during_time,
                   page.item page_item,
                   page.item_type page_item_type,
                   page.last_page_id,
                   page.page_id,
                   page.source_type,
                        concat(common.mid,'-', last_value(\\`if\\`(page.last_page_id is null,ts,null), true)  over(partition by common.mid order by ts )) session_id,
               date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'yyyy-MM-dd') date_id,
              date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') view_time
            from ods_log_inc
            where dt='$do_date'
            -- 过滤出页面日志
            and isnotnull(page.page_id)
      ),
     province_info  as(
              select
                      id   province_id,area_code
                from dim_province_full
                where dt='$do_date'

     )
insert overwrite table dwd_traffic_page_view_inc partition (dt='$do_date')
select
       province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_id,
       view_time,
       session_id,
       during_time
from log_info left join  province_info
on log_info.ar = province_info.area_code;
"

dwd_user_login_inc="
 insert overwrite table dwd_user_login_inc partition (dt='$do_date')
        select
               user_id,
               date_id,
               login_time,
               channel,
               province_id,
               version_code,
               mid_id,
               brand,
               model,
               operate_system
        from (-- 保证dwd_traffic_page_view_inc 已经导入完成
                 select user_id,
                        dt                                                        date_id,
                        view_time                                                 login_time,
                        channel,
                        province_id,
                        version_code,
                        mid_id,
                        brand,
                        model,
                        operate_system,
                        rank() over (partition by session_id order by view_time ) rn
                 from dwd_traffic_page_view_inc
                 where dt = '$do_date'
                   and isnotnull(user_id)
             ) tmp
        where rn = 1;
"
dwd_user_register_inc="
with
    -- 查询6-15日，有哪些已经注册的用户
    user_info as ( 
        
          select
                   data.id user_id,
                   data.create_time
            from ods_user_info_inc
            where dt='$do_date' and type='insert'
        
    ),
      mid_info as (
            -- 保证dwd_traffic_page_view_inc 已经导入完成
              select
                     channel,
                   province_id,
                   version_code,
                   mid_id,
                   brand,
                   model,
                   operate_system,
                     user_id
              from dwd_traffic_page_view_inc
              where dt='$do_date'
              and page_id = 'register'
                -- 筛选注册成功的
                and isnotnull(user_id)

      )
 insert overwrite table dwd_user_register_inc partition (dt='$do_date')
select
       user_info.user_id,
       '$do_date' date_id,
       create_time,
       -- log中有   6-14日才开始采集
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from user_info left join mid_info
on user_info.user_id = mid_info.user_id;
"
dwd_trade_trade_flow_acc="
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    nvl(oi.payment_date_id,pi.payment_date_id),
    nvl(oi.payment_time,pi.payment_time),
    nvl(oi.finish_date_id,log.finish_date_id),
    nvl(oi.finish_time,log.finish_time),
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    nvl(oi.payment_amount,pi.payment_amount),
    nvl(nvl(oi.finish_time,log.finish_time),'9999-12-31')
from
(
    select
        order_id,
        user_id,
        province_id,
        order_date_id,
        order_time,
        payment_date_id,
        payment_time,
        finish_date_id,
        finish_time,
        order_original_amount,
        order_activity_amount,
        order_coupon_amount,
        order_total_amount,
        payment_amount
    from dwd_trade_trade_flow_acc
    where dt='9999-12-31'
    union all
    select
        data.id,
        data.user_id,
        data.province_id,
        date_format(data.create_time,'yyyy-MM-dd') order_date_id,
        data.create_time,
        null payment_date_id,
        null payment_time,
        null finish_date_id,
        null finish_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount,
        null payment_amount
    from ods_order_info_inc
    where dt='$do_date'
    and type='insert'
)oi
left join
(
    select
        data.order_id,
        date_format(data.callback_time,'yyyy-MM-dd') payment_date_id,
        data.callback_time payment_time,
        data.total_amount payment_amount
    from ods_payment_info_inc
    where dt='$do_date'
    and type='update'
    and array_contains(map_keys(old),'payment_status')
    and data.payment_status='1602'
)pi
on oi.order_id=pi.order_id
left join
(
    select
        data.order_id,
        date_format(data.operate_time,'yyyy-MM-dd') finish_date_id,
        data.operate_time finish_time
    from ods_order_status_log_inc
    where dt='$do_date'
    and type='insert'
    and data.order_status='1004'
)log
on oi.order_id=log.order_id;

"

case $1 in
    "dwd_trade_cart_add_inc" )
        hive --database gmall -e "$dwd_trade_cart_add_inc"
    ;;
    "dwd_trade_order_detail_inc" )
        hive --database gmall -e "$dwd_trade_order_detail_inc"
    ;;
    "dwd_trade_pay_detail_suc_inc" )
        hive --database gmall -e "$dwd_trade_pay_detail_suc_inc"
    ;;
    "dwd_trade_cart_full" )
        hive --database gmall -e "$dwd_trade_cart_full"
    ;;   
    "dwd_trade_trade_flow_acc" )
        hive --database gmall -e "$dwd_trade_trade_flow_acc"
    ;;  
    "dwd_tool_coupon_used_inc" )
        hive --database gmall -e "$dwd_tool_coupon_used_inc"
    ;;
    "dwd_interaction_favor_add_inc" )
        hive --database gmall -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_traffic_page_view_inc" )
        hive --database gmall -e "$dwd_traffic_page_view_inc"
    ;;
    "dwd_user_register_inc" )
        hive --database gmall -e "$dwd_user_register_inc"
    ;;   
    "dwd_user_login_inc" )
        hive --database gmall -e "$dwd_user_login_inc"
    ;; 
    "all" )
        hive --database gmall -e "$dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_pay_detail_suc_inc$dwd_trade_cart_full$dwd_trade_trade_flow_acc$dwd_tool_coupon_used_inc$dwd_interaction_favor_add_inc$dwd_traffic_page_view_inc$dwd_user_register_inc$dwd_user_login_inc"
    ;;
esac
