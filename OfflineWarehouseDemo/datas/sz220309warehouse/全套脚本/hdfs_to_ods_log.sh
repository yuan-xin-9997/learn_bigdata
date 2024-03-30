#!/bin/bash
#使用: 脚本 日期
if [ -n "$1"  ]
then
	do_date=$1
else
	do_date=$(date -d yesterday +'%F')
fi

echo $do_date

hql="
load data inpath '/origin_data/gmall/log/topic_log/$do_date' overwrite into table ods_log_inc partition (dt='$do_date');
"

#执行命令判断要写入的路径是否存在
hadoop fs -test -e /warehouse/gmall/ods/ods_log_inc/dt=$do_date

if [ $? -eq 1 ]
then 
	hive -e "$hql" --database gmall
else
	echo 当前路径已经存在，无需重复导入
fi
	

