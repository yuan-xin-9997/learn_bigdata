#!/bin/bash
if [ $# -eq 0 ]
then
    echo -e "请输入参数：\nstart  启动Hadoop集群；\nstop  关闭Hadoop集群；"&&exit
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " ------------------- 启动 hdfs   集群 -------------------"
        ssh hadoop102 "/opt/module/hadoop/sbin/start-dfs.sh"
        echo " ------------------- 启动 yarn   集群  ------------------"
        ssh hadoop103 "/opt/module/hadoop/sbin/start-yarn.sh"
        echo " ------------------- 启动 historyserver ----------------"
        ssh hadoop102 "/opt/module/hadoop/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="

        echo " ------------------- 关闭 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop/bin/mapred --daemon stop historyserver"
        echo " ------------------- 关闭 yarn ---------------"
        ssh hadoop103 "/opt/module/hadoop/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop102 "/opt/module/hadoop/sbin/stop-dfs.sh"
;;
*)
    echo -e "请输入参数：\nstart  启动Hadoop集群；\nstop  关闭Hadoop集群；"&&exit
;;
esac
