#!/bin/bash
# ODS层->DIM层 首日数据装载脚本
# ods_to_dim_init.sh all/表名 [日期]
# todo 首日加载 在 全量快照表/拉链表 的区别

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

# 3. 根据第一个参数匹配加载