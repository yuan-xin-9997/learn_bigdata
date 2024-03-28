#!/bin/bash
# DWS层->ADS层 每日数据装载脚本
# dws_to_ads.sh all/表名 [日期]

# 1. 判断参数是否传入
if [ $# -lt 1 ]
then
    echo "必须至少传入一个表名/all..."
    exit
fi

# 2. 判断日期是否传入，如果传入日期则加载指定日期的数据，如果没有传入则加载前一天的数据
if [ "$2" == "" ];then
#     datestr`date -d '-1 day' +%F`
    datestr=$(date -d '-1 day' +%F)
else
    datestr=$2
fi
# 也可以换成这种写法
# [ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)
echo "当前要操作的日期是${datestr}"


# sql 语句
ads_coupon_stats
ads_new_order_user_stats
ads_order_by_province
ads_order_continuously_user_count
ads_order_stats_by_cate
ads_order_stats_by_tm
ads_order_to_pay_interval_avg
ads_page_path
ads_repeat_purchase_by_tm
ads_sku_cart_num_top3_by_cate
ads_sku_favor_count_top3_by_tm
ads_traffic_stats_by_channel
ads_user_action
ads_user_change
ads_user_retention
ads_user_stats







case $1 in
"ads_coupon_stats")
  /opt/module/hive/bin/hive --database gmal -e "$ads_coupon_stats"
  ;;
"ads_new_order_user_stats")
  /opt/module/hive/bin/hive --database gmal -e "$ads_new_order_user_stats"
  ;;
"ads_order_by_province")
  /opt/module/hive/bin/hive --database gmal -e "$ads_order_by_province"
  ;;
"ads_order_continuously_user_count")
  /opt/module/hive/bin/hive --database gmal -e "$ads_order_continuously_user_count"
  ;;
"ads_order_stats_by_cate")
  /opt/module/hive/bin/hive --database gmal -e "$ads_order_stats_by_cate"
  ;;
"ads_order_stats_by_tm")
  /opt/module/hive/bin/hive --database gmal -e "$ads_order_stats_by_tm"
  ;;
"ads_order_to_pay_interval_avg")
  /opt/module/hive/bin/hive --database gmal -e "$ads_order_to_pay_interval_avg"
  ;;
"ads_page_path")
  /opt/module/hive/bin/hive --database gmal -e "$ads_page_path"
  ;;
"ads_repeat_purchase_by_tm")
  /opt/module/hive/bin/hive --database gmal -e "$ads_repeat_purchase_by_tm"
  ;;
"ads_sku_cart_num_top3_by_cate")
  /opt/module/hive/bin/hive --database gmal -e "$ads_sku_cart_num_top3_by_cate"
  ;;
"ads_sku_favor_count_top3_by_tm")
  /opt/module/hive/bin/hive --database gmal -e "$ads_sku_favor_count_top3_by_tm"
  ;;
"ads_traffic_stats_by_channel")
  /opt/module/hive/bin/hive --database gmal -e "$ads_traffic_stats_by_channel"
  ;;
"ads_user_action")
  /opt/module/hive/bin/hive --database gmal -e "$ads_user_action"
  ;;
"ads_user_change")
  /opt/module/hive/bin/hive --database gmal -e "$ads_user_change"
  ;;
"ads_user_retention")
  /opt/module/hive/bin/hive --database gmal -e "$ads_user_retention"
  ;;
"ads_user_stats")
  /opt/module/hive/bin/hive --database gmal -e "$ads_user_stats"
  ;;
"all")
  /opt/module/hive/bin/hive --database gmal -e "${ads_coupon_stats};${ads_new_order_user_stats};${ads_order_by_province};${ads_order_continuously_user_count};${ads_order_stats_by_cate};${ads_order_stats_by_tm};${ads_order_to_pay_interval_avg};${ads_page_path};${ads_repeat_purchase_by_tm};${ads_sku_cart_num_top3_by_cate};${ads_sku_favor_count_top3_by_tm};${ads_traffic_stats_by_channel};${ads_user_action};${ads_user_change};${ads_user_retention};${ads_user_stats};"
  ;;
esac