#!/bin/bash
hadoop.sh start
zk start
kafka.sh start
maxwell.sh start
start-hbase.sh
flume.sh start
redis-server /etc/redis.conf
/opt/module/flink-1.13.6/bin/yarn-session.sh -d
maxwell.sh stop
maxwell.sh start