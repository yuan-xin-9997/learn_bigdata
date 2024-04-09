#!/bin/bash

case $1 in
"start"){
        echo ================== 启动 集群 ==================

        #启动 Zookeeper集群
        zk.sh start

        #启动 Hadoop集群
        my_hadoop.sh start

        #启动 Kafka采集集群
        kafka.sh start

        #启动采集 Flume
        flume1.sh start

        #启动日志消费 Flume
        flume2.sh start

        #启动业务消费 Flume
        flume3.sh start

        #启动 maxwell
        maxwell.sh start

        };;
"stop"){
        echo ================== 停止 集群 ==================

        #停止 Maxwell
        maxwell.sh stop

        #停止 业务消费Flume
        flume3.sh stop

        #停止 日志消费Flume
        flume2.sh stop

        #停止 日志采集Flume
        flume1.sh stop

        #停止 Kafka采集集群
        kafka.sh stop

        #停止 Hadoop集群
        my_hadoop.sh stop

        #停止 Zookeeper集群
        zk.sh stop

};;
"show"|"status"){
        echo ================== 显示 集群 ==================

        #停止 Maxwell
        maxwell.sh show

        #停止 日志采集Flume
        flume1.sh show

        #停止 日志消费Flume
        flume2.sh show

        #停止 业务消费Flume
        flume3.sh show

        #停止 Kafka采集集群
        kafka.sh show

        #停止 Hadoop集群
        my_hadoop.sh show

        #停止 Zookeeper集群
        zk.sh show

};;
esac