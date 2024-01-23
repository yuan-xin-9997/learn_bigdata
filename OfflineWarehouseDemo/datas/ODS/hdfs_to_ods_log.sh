#!/bin/bash
# hdfs_to_ods_log.sh [日期]
#    desc: ODS 日志数据加载脚本，从HDFS导入数据到Hive表中
# 1、判断参数是否传入，如果传入使用指定日期，如果没有传入使用前一天日期
if [ "$1" == "" ];then
#     datestr`date -d '-1 day' +%F`
    datestr=$(date -d '-1 day' +%F)
else
    datestr=$1
fi

# 2、执行加载语句，加载数据到ods_log_inc表中
path="/origin_data/gmall/log/topic_log/$datestr"
/opt/module/hive/bin/hive -e "load data inpath '$path' overwrite into table gmall.ods_log_inc partition (dt='$datestr')"
