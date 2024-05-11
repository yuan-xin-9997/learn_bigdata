#!/bin/bash
# hdfs_to_ods_db.sh all|表名  [日期]
#    desc: ODS 业务数据加载脚本，从HDFS导入数据到Hive表中
# 1. 判断参数是否传入
if [ $# -lt 1 ];then
    echo "usage: hdfs_to_ods_db.sh all|表名  [日期]"
    exit 1
fi
# 2. 判断日期是否传入，如果传入使用指定日期，如果没有传入使用前一天日期
if [ "$2" == "" ];then
#     datestr`date -d '-1 day' +%F`
    datestr=$(date -d '-1 day' +%F)
else
    datestr=$2
fi

# 导入HDFS函数
function loadData(){
    sql="use gmall;"
    tableNames=$*
    for tableName in $tableNames
    do
        path="/origin_data/gmall/db/${tableName:4}/$datestr"
        # 判断path是否存在
        hadoop fs -test -e $path
        if [ $? -eq 0 ];then
            echo "$path is exits."
            sql="$sql;load data inpath '$path' overwrite into table ${tableName} partition (dt='$datestr')"
        else
            echo "$path is not exits."
        fi
    done
    /opt/module/hive/bin/hive -e "$sql"
}

# 3. 根据第一个参数匹配加载数据到ODS
case $1 in
"all")
   loadData "ods_activity_info_full" "ods_activity_rule_full" "ods_base_category1_full" "ods_base_category2_full" "ods_base_category3_full" "ods_base_dic_full" "ods_base_province_full" "ods_base_region_full" "ods_base_trademark_full" "ods_cart_info_full" "ods_cart_info_inc" "ods_comment_info_inc" "ods_coupon_info_full" "ods_coupon_use_inc" "ods_favor_info_inc" "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc" "ods_order_detail_inc" "ods_order_info_inc" "ods_order_refund_info_inc" "ods_order_status_log_inc" "ods_payment_info_inc" "ods_refund_payment_inc" "ods_sku_attr_value_full" "ods_sku_info_full" "ods_sku_sale_attr_value_full" "ods_spu_info_full" "ods_user_info_inc"
;;
"ods_activity_info_full")
    loadData $1
;;
"ods_activity_rule_full")
    loadData $1
;;
"ods_base_category1_full")
    loadData $1
;;
"ods_base_category2_full")
    loadData $1
;;
"ods_base_category3_full")
    loadData $1
;;
"ods_base_dic_full")
    loadData $1
;;
"ods_base_province_full")
    loadData $1
;;
"ods_base_region_full")
loadData $1
;;
"ods_base_trademark_full")
loadData $1
;;
"ods_cart_info_full")
loadData $1
;;
"ods_cart_info_inc")
loadData $1
;;
"ods_comment_info_inc")
loadData $1;;
"ods_coupon_info_full")
loadData $1;;
"ods_coupon_use_inc")
loadData $1;;
"ods_favor_info_inc")
loadData $1;;
"ods_order_detail_activity_inc")
loadData $1;;
"ods_order_detail_coupon_inc")
loadData $1;;
"ods_order_detail_inc")
loadData $1;;
"ods_order_info_inc")
loadData $1;;
"ods_order_refund_info_inc")
loadData $1;;
"ods_order_status_log_inc")
loadData $1;;
"ods_payment_info_inc")
loadData $1;;
"ods_refund_payment_inc")
loadData $1;;
"ods_sku_attr_value_full")
loadData $1;;
"ods_sku_info_full")
loadData $1;;
"ods_sku_sale_attr_value_full")
loadData $1;;
"ods_spu_info_full")
loadData $1;;
"ods_user_info_inc")
loadData $1;;
*)
    echo "参数输入错误"
;;
esac