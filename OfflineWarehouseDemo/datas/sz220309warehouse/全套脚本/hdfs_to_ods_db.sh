#!/bin/bash
#使用: 脚本 日期 ods_表名|all
if [ -n "$2"  ]
then
	do_date=$2
else
	do_date=$(date -d yesterday +'%F')
fi

#把向表中导入数据的功能封装为一个函数
function load_data(){

hql=""
#循环遍历函数的参数列表，取出每一个表名
for tableName in $*
do
	#activity_info_full 和 ods_activity_info_full 
	hdfs_path=${tableName:4}
	#判断hdfs上ods层这张表要导入的路径是否已经存在，如果不存在再导入
	hadoop fs -test -e /warehouse/gmall/ods/$tableName/dt=$do_date

	if [ $? -eq 1 ]
	then
		hql=$hql"load data inpath '/origin_data/gmall/db/$hdfs_path/$do_date' overwrite into table $tableName  partition (dt='$do_date');"
	fi

done

#批量执行
hive -e "$hql" --database gmall
  

}

#判断脚本传入的第二个参数，是某个表名还是all，如果是某个表名，单独向某个表导入数据，否则向所有表导入数据
#多条件判断用什么?  case 或 嵌套的if-else if()......
case $1 in
"ods_activity_info_full")
      load_data ods_activity_info_full
      ;;
"ods_activity_rule_full")
      load_data ods_activity_rule_full
      ;;
"ods_base_category1_full")
      load_data ods_base_category1_full
      ;;
"ods_base_category2_full")
      load_data ods_base_category2_full
      ;;
"ods_base_category3_full")
      load_data ods_base_category3_full
      ;;
"ods_base_dic_full")
      load_data ods_base_dic_full
      ;;
"ods_base_province_full")
      load_data ods_base_province_full
      ;;
"ods_base_region_full")
      load_data ods_base_region_full
      ;;
"ods_base_trademark_full")
      load_data ods_base_trademark_full
      ;;
"ods_cart_info_full")
      load_data ods_cart_info_full
      ;;
"ods_cart_info_inc")
      load_data ods_cart_info_inc
      ;;
"ods_comment_info_inc")
      load_data ods_comment_info_inc
      ;;
"ods_coupon_info_full")
      load_data ods_coupon_info_full
      ;;
"ods_coupon_use_inc")
      load_data ods_coupon_use_inc
      ;;
"ods_favor_info_inc")
      load_data ods_favor_info_inc
      ;;
"ods_order_detail_activity_inc")
      load_data ods_order_detail_activity_inc
      ;;
"ods_order_detail_coupon_inc")
      load_data ods_order_detail_coupon_inc
      ;;
"ods_order_detail_inc")
      load_data ods_order_detail_inc
      ;;
"ods_order_info_inc")
      load_data ods_order_info_inc
      ;;
"ods_order_refund_info_inc")
      load_data ods_order_refund_info_inc
      ;;
"ods_order_status_log_inc")
      load_data ods_order_status_log_inc
      ;;
"ods_payment_info_inc")
      load_data ods_payment_info_inc
      ;;
"ods_refund_payment_inc")
      load_data ods_refund_payment_inc
      ;;
"ods_sku_attr_value_full")
      load_data ods_sku_attr_value_full
      ;;
"ods_sku_info_full")
      load_data ods_sku_info_full
      ;;
"ods_sku_sale_attr_value_full")
      load_data ods_sku_sale_attr_value_full
      ;;
"ods_spu_info_full")
      load_data ods_spu_info_full
      ;;
"ods_user_info_inc")
      load_data ods_user_info_inc
      ;;
"all")
      load_data ods_activity_info_full ods_activity_rule_full ods_base_category1_full ods_base_category2_full ods_base_category3_full ods_base_dic_full ods_base_province_full ods_base_region_full ods_base_trademark_full ods_cart_info_full ods_cart_info_inc ods_comment_info_inc ods_coupon_info_full ods_coupon_use_inc ods_favor_info_inc ods_order_detail_activity_inc ods_order_detail_coupon_inc ods_order_detail_inc ods_order_info_inc ods_order_refund_info_inc ods_order_status_log_inc ods_payment_info_inc ods_refund_payment_inc ods_sku_attr_value_full ods_sku_info_full ods_sku_sale_attr_value_full ods_spu_info_full ods_user_info_inc
    ;;
esac


