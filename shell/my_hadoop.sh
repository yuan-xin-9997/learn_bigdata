#!/bin/bash

case $1 in
"start")
	# 启动HDFS
	ssh hadoop102 ${HADOOP_HOME}/sbin/start-dfs.sh
	# 启动YARN
	ssh hadoop103 ${HADOOP_HOME}/sbin/start-yarn.sh
	# 启动历史服务器
	mapred --daemon start historyserver
	;;
"stop")
        # 关闭HDFS
        ssh hadoop102 ${HADOOP_HOME}/sbin/stop-dfs.sh
        # 关闭YARN
        ssh hadoop103 ${HADOOP_HOME}/sbin/stop-yarn.sh
	# 关闭历史服务器
	mapred --daemon stop historyserver
	;;
"restart")
	my_hadoop.sh stop
	my_hadoop.sh start
	;;
"show"|"status")
	jpsall.sh
	;;
*)
	echo "输入的参数不对!!!"
esac
