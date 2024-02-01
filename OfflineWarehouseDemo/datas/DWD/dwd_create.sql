-- 交易域加购事务事实表（增量表）
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码（冗余）',
    `source_type_name` STRING COMMENT '来源类型名称（冗余）',
    `sku_num`          BIGINT COMMENT '加购物车件数（冗余）'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy')
;

-- 加载数据
---  首日加载：数据放入事实发生对应的日期分区中
---      分区：保存当日发生的事实
---      数据来源：ODS_cart_info_full[2020-06-14]
---      去处：根据加购物车的时间放入对应的分区中
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

---  每日加载：数据放入事实表当日分区
---      分区：保存当日发生的事实
---      数据来源：ODS_cart_info_full[指定日期]
---      去处：放入[指定日期]对应的分区中
with cart as (
    select
        data.id,
        data.user_id,
        data.sku_id,
        -- 事件时间，加购物车的时间，基于ts
        date_format(from_utc_timestamp(ts*1000, "Asia/Shanghai"), 'yyyy-MM-dd') date_id,
        -- 加购物车时间，精确到时分秒，基于ts
        date_format(from_utc_timestamp(ts*1000, "Asia/Shanghai"), 'yyyy-MM-dd HH:mm:ss') create_time,
        data.source_id,
        data.source_type,
        -- data.source_type_name, -- 从字典表查询
        data.sku_num
    from ods_cart_info_inc
    where dt='2020-06-15' and
          (type='insert' or
           (type='update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as bigint)) )
), bc as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-15' and parent_code='24'
)
insert overwrite  table dwd_trade_cart_add_inc partition (dt='2020-06-15')
select
    id,
    user_id,
    sku_id,
    date_id,
    create_time,
    source_id,
    source_type,
    source_type_name,
    sku_num -- 不需要动态分区
from cart left join bc on cart.source_type = bc.dic_code;


-- 交易域下单事务事实表（增量表）
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',  -- 来源order_detail订单明细表
    `order_id`              STRING COMMENT '订单id',  -- 来源order_detail订单明细表
    `user_id`               STRING COMMENT '用户id',  -- 来源order_info订单表
    `sku_id`                STRING COMMENT '商品id', -- 来源订单明细表（order_detail）
    `province_id`           STRING COMMENT '省份id',  -- 来源order_info订单表
    `activity_id`           STRING COMMENT '参与活动规则id',  -- 来源订单明细活动关联表order_datail_activity
    `activity_rule_id`      STRING COMMENT '参与活动规则id',  -- 来源订单明细活动关联表order_datail_activity
    `coupon_id`             STRING COMMENT '使用优惠券id', -- 来源 订单明细优惠券关联表order_datail_coupon
    `date_id`               STRING COMMENT '下单日期id',  -- 来源order_detail订单明细表create_time
    `create_time`           STRING COMMENT '下单时间',  -- 来源order_detail订单明细表create_time
    `source_id`             STRING COMMENT '来源编号',  -- 来源order_detail订单明细表
    `source_type_code`      STRING COMMENT '来源类型编码',  -- 来源order_detail订单明细表
    `source_type_name`      STRING COMMENT '来源类型名称',  -- 来源base_dic
    `sku_num`               BIGINT COMMENT '商品数量', -- 来源order_detail订单明细表
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',  -- 来源order_detail订单明细表
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',  -- 来源order_detail订单明细表
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',  -- 来源order_detail订单明细表
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'  -- 来源order_detail订单明细表
) COMMENT '交易域下单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 加载数据

---  首日加载：数据放入事实发生对应的日期分区中
---      分区：保存当日下单事实行为数据
---      数据来源：ODS_多个表[2020-06-14]
---      去处：根据下单的时间放入对应的分区中
---      粒度：一个用户一个订单一个商品
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

---  每日加载
---  业务对数据的影响：
--       下单的时候会向ods_order_detail_inc插入N条数据[有多少商品就有多少条数据]
--       下单的时候会向ods_order_info_inc插入1条数据
--       下单的时候会向ods_order_detail_activity_inc插入N条数据[订单中一个商品参加一个活动插入一条]
--       下单的时候会向ods_order_detail_coupon_inc插入N条数据[订单中一个商品使用一个优惠券插入一条]
--   去处：放入[指定日期]对应的分区中
with od as (
    -- 下单的时候会向ods_order_detail_inc插入N条数据[有多少商品就有多少条数据]
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
    from ods_order_detail_inc where dt='2020-06-15' and type='insert'
), oi as (
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info_inc where dt='2020-06-15' and type='insert'
), oda as (
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
       from ods_order_detail_activity_inc where dt = '2020-06-15' and type='insert'
        ) t1 where rn =1
), odc as (
    select
        order_detail_id,
        coupon_id
    from (
        select
            data.order_detail_id,
            data.coupon_id,
            data.create_time,
            row_number() over (partition by data.order_detail_id order by data.create_time asc) rn
        from ods_order_detail_coupon_inc where dt = '2020-06-15' and type='insert'
    )   t1 where rn =1
), dc as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-15' and parent_code='24'
)
insert overwrite table dwd_trade_order_detail_inc partition (dt='2020-06-15')
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
    split_total_amount
    -- date_id -- 动态分区字段
from od left join oi
on od.order_id = oi.id
left join oda on od.id = oda.order_detail_id
left join odc on od.id = odc.order_detail_id
left join dc on od.source_type_code=dc.dic_code
;


-- 交易域支付成功事务事实表（增量表）
--    粒度：用户+订单+商品
--    分区：每个日期分区保存当日的支付成功行为
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    -- order_detail
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    -- order_info
    `user_id`               STRING COMMENT '用户id',
    -- order_detail
    `sku_id`                STRING COMMENT '商品id',
    -- order_info
    `province_id`           STRING COMMENT '省份id',
    -- order_detail_activity
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    -- order_detail_coupon
    `coupon_id`             STRING COMMENT '使用优惠券id',
    -- payment_info
    `payment_type_code`     STRING COMMENT '支付类型编码',
    -- base_dic
    `payment_type_name`     STRING COMMENT '支付类型名称',
    -- payment_info
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         STRING COMMENT '支付成功时间',
    -- order_detail
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    -- base_dic
    `source_type_name`      STRING COMMENT '来源类型名称',
    -- order_detail
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
     -- payment_info
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域成功支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据加载
---  首日数据加载
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

---  每日数据加载
---     支付成功对业务的影响：
---           1. 支付成功会更新payment_info中的支付记录的状态、callback_time时间
---           2. 支付成功会更新order_info表和order_status字段，operator_time字段
with od as (
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
        data.split_total_amount split_payment_amount,
        dt
    -- 支付成功的订单的商品数据可能在当天，也可能在前一天（此处默认不超过1天），下订单之后如果30分钟内没有支付，则订单过期
    from ods_order_detail_inc where (dt='2020-06-15' or dt=date_sub('2020-06-15', 1)) and (type='insert' or type='bootstrap-insert')
), py as (
    select
        data.order_id,
        data.payment_type payment_type_code,
        -- data.total_amount split_payment_amount,
        date_format(data.callback_time, 'yyyy-MM-dd') date_id,
        data.callback_time
    -- 注意此处，查询old字段(map)中包含callback_time这一key的字段，并且data字段的callback_time不为null
    from ods_payment_info_inc where dt='2020-06-15' and type='update' and array_contains(map_keys(old), 'callback_time') and data.callback_time is not null
), oi as (
    select
        data.id,
        data.user_id,
        data.province_id
    -- 查询支付状态从未支付到已支付的
    from ods_order_info_inc where dt='2020-06-15' and type='update' and old['order_status']='1001' and data.order_status='1002'
), oda as (
    -- 一行代表订单中一个商品参加的优惠活动
    -- 生成的数据有问题，一个订单中一个商品可能使用了多张优惠券，取其中一条即可
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
        -- 支付成功的订单的商品参加活动的数据可能在当天，也可能在前一天（此处默认不超过1天），下订单之后如果30分钟内没有支付，则订单过期
        --     支付成功的订单的商品如果是当天下单支付，则从ODS分区中获取insert数据，如果是昨天下单今天支付，应该从前一天分区中获取商品活动信息
        --     type=insert，如果前一天是首日此时type=bootstrap-insert
        from ods_order_detail_activity_inc where (dt='2020-06-15' or dt=date_sub('2020-06-15', 1)) and (type='insert' or type='bootstrap-insert')
    )   t1 where rn =1
),odc as (
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
        from ods_order_detail_coupon_inc where (dt='2020-06-15' or dt=date_sub('2020-06-15', 1)) and (type='insert' or type='bootstrap-insert')
    )   t1 where rn =1
), dc1 as (
    select
        dic_code,
        dic_name payment_type_name
    from ods_base_dic_full where dt='2020-06-15' and parent_code='11'
), dc2 as (
    select
        dic_code,
        dic_name source_type_name
    from ods_base_dic_full where dt='2020-06-15' and parent_code='24'
)
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
    split_payment_amount
from od inner join py on od.order_id=py.order_id
left join oi on od.order_id=oi.id
left join oda on od.order_id=oda.order_detail_id
left join odc on od.order_id=oda.order_detail_id
left join dc1 on py.payment_type_code=dc1.dic_code
left join dc2 on od.source_type_code=dc2.dic_code
;


-- 交易域购物车周期快照事实表
DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据加载，不用区分首日，每日，直接从ods层全量的购物车表中导入数据
insert overwrite table dwd_trade_cart_full partition (dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full where dt='2020-06-14' and is_ordered=0; -- 排除已经下单的购物车商品，购物车商品下单之后就不在购物车了


-- 工具域优惠券使用(支付)事务事实表
DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT 'user_id',
    `order_id`     STRING COMMENT 'order_id',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 首日数据加载
---  粒度：用户使用优惠券
---  分区：每个分区保存是每个用户每个优惠券用于支付的行为
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

-- 修复元数据
msck repair table ods_coupon_use_inc;

-- 每日数据加载
---   业务影响：订单中使用了优惠券，并且订单付款，此时会更新coupon_use表中的支付使用时间字段
insert overwrite table dwd_tool_coupon_used_inc partition (dt='2020-06-15')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time, 'yyyy-MM-dd') date_id,
    data.used_time payment_time
from ods_coupon_use_inc where dt='2020-06-15' and type='update' and array_contains(map_keys(old), 'used_time') and data.used_time is not null;


-- 互动域收藏商品事务事实表
DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'sku_id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 粒度：用户收藏商品的行为
-- 分区：用户当日收藏商品的行为

-- 数据加载
-- 首日加载
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

-- 每日加载
insert overwrite table dwd_interaction_favor_add_inc partition (dt='2020-06-15')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time, 'yyyy-MM-dd') date_id,
    data.create_time
from ods_favor_info_inc where dt='2020-06-15' and type='insert';


-- 交易域交易流程累积快照事实表
DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `province_id`           STRING COMMENT '省份id',
    `order_date_id`         STRING COMMENT '下单日期id',
    `order_time`            STRING COMMENT '下单时间',
    `payment_date_id`       STRING COMMENT '支付日期id',
    `payment_time`          STRING COMMENT '支付时间',
    `finish_date_id`        STRING COMMENT '确认收货日期id',
    `finish_time`           STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`        DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
TBLPROPERTIES ('orc.compress' = 'snappy');

-- 粒度：订单，不是商品，粒度的确定根据需求来决定
-- 分区：
--    9999-12-31：截止当天为止未完成（收货）的流程
--    普通日期分区：保存当天完成的分区
-- 数据来源：
--    首日：从ODS层首日分区直接获取
--    每日：从ODS层当日分区直接获取 + 9999-12-31分区

-- 数据加载
-- 首日加载
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
    `if`(finish_date_id is not null, finish_date_id, '9999-12-31') -- 动态分区字段，
from oi left join py on oi.order_id=py.order_id
left join os on oi.order_id=os.order_id
;

-- 每日加载
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
    -- 从订单表中查询当日分区插入的数据，不需要查询update的数据，可以从订单流水表中查询
    from ods_order_info_inc where dt='2020-06-15' and type='insert'
)
























