#! /bin/bash
# ods_to_dwd_init.sh all/表名

init_data="2020-06-14"

# 1. 判断参数是否传入
if [ $# -lt 1 ]; then
    echo "必须传入 表名/all 参数"
fi

# 2. 根据第一个参数匹配，导入数据到dwd层
dwd_trade_cart_add_inc_sql="""
set hive.exec.dynamic.partition.mode=nonstrict; -- 若要使用动态分区，需要设置hive参数为非严格模式
with cart as (
  select
        data.id,
        data.user_id,
        data.sku_id,
        -- 首日不知道加了几次购物车，以第一次加购物时间作为事件发生时间
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time,
        data.source_id,
        data.source_type,
        -- data.source_type_name, -- 从字典表查询
        data.sku_num
  from ods_cart_info_inc where dt='2020-06-14' and type='bootstrap-insert' -- 注意：此处不是full，full是后续做 周期型快照事实表的时候使用的
), dc as (
    select
        dic_name source_type_name,
        dic_code
    from ods_base_dic_full where dt='2020-06-14' and parent_code='24'
)
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type,
    source_type_name,
    sku_num,
    date_id -- 动态分区字段
from cart
left join dc
on cart.source_type = dc.dic_code;
"""

dwd_trade_order_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nonstrict; -- 若要使用动态分区，需要设置hive参数为非严格模式
with od as (
    -- 一行代表一个商品的下单信息
    select
        data.id,
        data.order_id,
        data.sku_id,
        -- data.date_id,
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time,
        data.source_id,
        data.source_type source_type_code,
        data.sku_num,
        data.order_price*data.sku_num split_original_amount,
        data.split_activity_amount,
        data.split_coupon_amount,
        data.split_total_amount
    from ods_order_detail_inc where dt = '2020-06-14' and type='bootstrap-insert'
), oi as (
    -- 一行代表一个订单信息
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info_inc where dt = '2020-06-14' and type='bootstrap-insert'
), oda as (
   -- 一行代表订单中一个商品参加的活动
   -- 生成的数据有问题，一个订单中一个商品可能参加了多个相同活动，取其中一条即可
   select
       order_detail_id,
       activity_id,
       activity_rule_id
   from (
        select
           data.order_detail_id,
           data.activity_id,
           data.activity_rule_id,
           row_number() over (partition by data.order_detail_id order by data.create_time asc) rn
       from ods_order_detail_activity_inc where dt = '2020-06-14' and type='bootstrap-insert'
        ) t1 where rn =1
), odc as (
    -- 一行代表订单中一个商品参加的优惠活动
    -- 生成的数据有问题，一个订单中一个商品可能使用了多张优惠券，取其中一条即可
    select
        order_detail_id,
        coupon_id
    from (
        select
            data.order_detail_id,
            data.coupon_id,
            data.create_time,
            row_number() over (partition by data.order_detail_id order by data.create_time asc) rn
        from ods_order_detail_coupon_inc where dt = '2020-06-14' and type='bootstrap-insert'
    )   t1 where rn =1
), dc as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='24'
)
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_id -- 动态分区字段
from od left join oi
on od.order_id = oi.id
left join oda on od.id = oda.order_detail_id
left join odc on od.id = odc.order_detail_id
left join dc on od.source_type_code=dc.dic_code
;
"

dwd_trade_pay_detail_suc_inc_sql="
set hive.exec.dynamic.partition.mode=nonstrict; -- 若要使用动态分区，需要设置hive参数为非严格模式
with od as (
    -- 一行代表一个商品的下单信息
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.source_id,
        data.source_type source_type_code,
        data.sku_num,
        data.order_price*data.sku_num split_original_amount,
        data.split_activity_amount,
        data.split_coupon_amount,
        data.split_total_amount split_payment_amount
    from ods_order_detail_inc where dt = '2020-06-14' and type='bootstrap-insert'
), oi as (
    -- 一行代表一个订单信息
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info_inc where dt = '2020-06-14' and type='bootstrap-insert'
), oda as (
   -- 一行代表订单中一个商品参加的活动
   -- 生成的数据有问题，一个订单中一个商品可能参加了多个相同活动，取其中一条即可
   select
       order_detail_id,
       activity_id,
       activity_rule_id
   from (
        select
           data.order_detail_id,
           data.activity_id,
           data.activity_rule_id,
           row_number() over (partition by data.order_detail_id order by data.create_time asc) rn
       from ods_order_detail_activity_inc where dt = '2020-06-14' and type='bootstrap-insert'
        ) t1 where rn =1
), odc as (
    -- 一行代表订单中一个商品参加的优惠活动
    -- 生成的数据有问题，一个订单中一个商品可能使用了多张优惠券，取其中一条即可
    select
        order_detail_id,
        coupon_id
    from (
        select
            data.order_detail_id,
            data.coupon_id,
            data.create_time,
            row_number() over (partition by data.order_detail_id order by data.create_time asc) rn
        from ods_order_detail_coupon_inc where dt = '2020-06-14' and type='bootstrap-insert'
    )   t1 where rn =1
), py as (
    -- 支付表
    select
        data.order_id,
        data.payment_type payment_type_code,
        -- data.total_amount split_payment_amount,
        date_format(data.callback_time, 'yyyy-MM-dd') date_id,
        data.callback_time
    -- 只有支付成功callback_time才有值
    from ods_payment_info_inc where dt = '2020-06-14' and type='bootstrap-insert' and data.callback_time is not null
), dc1 as (
    select
        dic_code,
        dic_name payment_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='11'
), dc2 as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-14' and parent_code='24'
)
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_payment_amount,
    date_id  -- 动态分区字段
from od inner join py on od.order_id=py.order_id -- 此处要内连接，因为订单详情表中会有支付失败的商品，此处只需要支付成功的数据
left join oi on od.order_id = oi.id
left join oda on od.id=oda.order_detail_id
left join odc on od.id=odc.order_detail_id
left join dc1 on py.payment_type_code=dc1.dic_code
left join dc2 on od.source_type_code=dc2.dic_code
;
"

dwd_trade_cart_full_sql="
insert overwrite table dwd_trade_cart_full partition (dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full where dt='2020-06-14' and is_ordered=0; -- 排除已经下单的购物车商品，购物车商品下单之后就不在购物车了
"

dwd_tool_coupon_used_inc_sql="
insert overwrite table dwd_tool_coupon_used_inc partition (dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time, 'yyyy-MM-dd') date_id,
    data.used_time payment_time,
    date_format(data.used_time, 'yyyy-MM-dd') date_id -- 动态分区字段
from ods_coupon_use_inc where dt='2020-06-14' and type='bootstrap-insert' and data.used_time is not null; -- 筛选已支付使用的优惠券
"

dwd_interaction_favor_add_inc_sql="
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time, 'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time, 'yyyy-MM-dd') date_id  -- 动态分区字段
from ods_favor_info_inc where dt='2020-06-14' and type='bootstrap-insert'
;
"

dwd_trade_trade_flow_acc_sql="""
set hive.exec.dynamic.partition.mode=nonstrict;
with oi as (
    select
        data.id order_id,
        data.user_id,
        data.province_id,
        date_format(data.create_time, 'yyyy-MM-dd') order_date_id,
        data.create_time order_time,
        data.original_total_amount order_original_amount,
        data.activity_reduce_amount order_activity_amount,
        data.coupon_reduce_amount order_coupon_amount,
        data.total_amount order_total_amount
    from ods_order_info_inc where dt='2020-06-14' and type='bootstrap-insert'
), py as (
    select
        data.order_id,
        date_format(data.callback_time, 'yyyy-MM-dd') payment_date_id,
        data.callback_time payment_time,
        data.total_amount  payment_amount
    -- 筛选callback_time不为null的，即支付成功的字段
    from ods_payment_info_inc where dt='2020-06-14' and type='bootstrap-insert' and data.callback_time is not null
), os as (
    select
        data.order_id,
        date_format(data.operate_time, 'yyyy-MM-dd') finish_date_id,
        data.operate_time finish_time
    -- 查询1004即已收货的订单+1003已经取消的订单
    from ods_order_status_log_inc where dt='2020-06-14' and type='bootstrap-insert' and (data.order_status='1004' or data.order_status='1003')
)
insert overwrite table dwd_trade_trade_flow_acc partition (dt)
select
    oi.order_id,
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
    payment_amount,
    -- finish_date_id有值表示已完成的订单，写入finish_date_id分区中，没有值表示未完成，写入9999-12-31分区中
    if(finish_date_id is not null, finish_date_id, '9999-12-31') -- 动态分区字段，
from oi left join py on oi.order_id=py.order_id
left join os on oi.order_id=os.order_id
;
"""

dwd_traffic_page_view_inc_sql="
set hive.cbo.enable=false;
with pg as (
      select ar,
             brand,
             channel,
             is_new,
             model,
             operate_system,
             user_id,
             version_code,
             page_item,
             page_item_type,
             last_page_id,
             page_id,
             source_type,
             during_time,
             date_id,
             view_time,
             -- 开窗
             last_value(session_point, true) over (partition by user_id order by ts asc) session_id
      from (
          select
            common.ar,
            common.ba brand,
            common.ch channel,
            common.is_new is_new,
            common.md model,
    --         common.mid mid_id,
            common.os operate_system,
            common.uid user_id,
            common.vc version_code,
            page.item page_item,
            page.item_type page_item_type,
            page.last_page_id last_page_id,
            page.page_id page_id,
            page.source_type source_type,
            page.during_time during_time,
            date_format(from_utc_timestamp(ts, 'Asia/Shanghai'), 'yyyy-MM-dd') date_id,
            date_format(from_utc_timestamp(ts, 'Asia/Shanghai'), 'yyyy-MM-dd HH:mm:ss') view_time,
            if(page.last_page_id is null, concat(common.uid, "_", ts), null) session_point,
            ts
        from ods_log_inc where dt='2020-06-14' and page is not null
      ) t1
), pv as (
    select
        id province_id,
        area_code
    from ods_base_province_full where dt='2020-06-14'
)
insert overwrite table dwd_traffic_page_view_inc partition (dt='2020-06-14')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
--     mid_id,
    null,
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
from pg left join pv on pg.ar = pv.area_code
;
"

dwd_user_register_inc_sql="
set hive.execution.engine=spark;
with us as (
    -- 查询截止2020-06-14所有用户注册数据
    select
        data.id user_id,
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time
    from ods_user_info_inc where dt='2020-06-14' and type='bootstrap-insert'
), lg as (
    -- 查询2020-06-14当天用户注册的设备信息，之前的无法拿到
    select
        common.ch channel,
        common.ar,
        common.vc version_code,
        common.ba brand,
        common.md model,
        common.os operate_system,
        common.uid
    from ods_log_inc where dt='2020-06-14' and common.uid is not null and page.page_id='register'
), pv as (
    -- 查询省份信息
    select
        id province_id,
        area_code
    from ods_base_province_full where dt='2020-06-14'
)
-- insert overwrite table dwd_user_register_inc partition (dt='2020-06-14')
insert overwrite table dwd_user_register_inc partition (dt)
select
    us.user_id,
    date_id,
    create_time,
    channel,
    province_id,
    version_code,
    null,
    brand,
    model,
    operate_system,
    date_id
from us left join lg on us.user_id=lg.uid
left join pv on lg.ar=pv.area_code;
"

dwd_user_login_inc_sql="
insert overwrite table dwd_user_register_inc partition (dt='2020-06-14')
select uid,
       date_id,
       login_time,
       channel,
       province_id,
       version_code,
       null,
       brand,
       model,
       operate_system
from (select uid,
             date_id,
             login_time,
             channel,
             ar,
             version_code,
             brand,
             model,
             operate_system,
             rn
      from (select channel,
                   ar,
                   version_code,
                   brand,
                   model,
                   operate_system,
                   uid,
                   date_id,
                   login_time,
                   -- 对session id开窗，只取每条会话登录的那一次页面点击
                   row_number() over (partition by seesion_id order by ts) rn
            from (select channel,
                         ar,
                         version_code,
                         brand,
                         model,
                         operate_system,
                         uid,
                         ts,
                         date_format(from_utc_timestamp(ts, "Asia/Shanghai"), 'yyyy-MM-dd HH:mm:ss') login_time,
                         date_format(from_utc_timestamp(ts, "Asia/Shanghai"), 'yyyy-MM-dd')          date_id,
                         last_page_id,
                         page_id,
                         -- 为每一次会话内的页面点击行为补全session id
                         last_value(session_point, true) over (partition by uid order by ts asc)     seesion_id
                  from (select common.ch                                                        channel,
                               common.ar,
                               common.vc                                                        version_code,
                               common.ba                                                        brand,
                               common.md                                                        model,
                               common.os                                                        operate_system,
                               common.uid,
                               page.last_page_id,
                               page.page_id,
                               ts,
                               -- 找到每个会话的起点
                               if(page.last_page_id is null, concat(common.uid, '-', ts), null) session_point
                        from ods_log_inc
                        where dt = '2020-06-14'
                          and page.page_id is not null) t1) t2
            where t2.uid is not null) t3  -- 一次会话中只要有uid称之为一次登录
      where rn = 1) t4
         left join (select id province_id,
                           area_code
                    from ods_base_province_full
                    where dt = '2020-06-14') t5 on t4.ar = t5.area_code;
"

case $1 in
"all")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_interaction_favor_add_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_tool_coupon_used_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_cart_add_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_cart_full_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_order_detail_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_pay_detail_suc_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_trade_flow_acc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_traffic_page_view_inc_sql"
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_user_register_inc_sql"
;;

"dwd_interaction_favor_add_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_interaction_favor_add_inc_sql_sql"
;;

"dwd_tool_coupon_used_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_tool_coupon_used_inc_sql"
;;

"dwd_trade_cart_add_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_cart_add_inc_sql"
;;

"dwd_trade_cart_full")
        /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_cart_full_sql"

;;

"dwd_trade_order_detail_inc")
 /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_order_detail_inc_sql"

;;

"dwd_trade_pay_detail_suc_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_pay_detail_suc_inc_sql"

;;

"dwd_trade_trade_flow_acc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_trade_trade_flow_acc_sql"

;;

"dwd_traffic_page_view_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_traffic_page_view_inc_sql"

;;

"dwd_user_register_inc")
    /opt/module/hive/bin/hive -e "use gmall; set hive.exec.dynamic.partition.mode=nonstrict; $dwd_user_register_inc_sql"
;;

*)
echo "参数错误"
;;

esac
