#!/bin/bash

case $1 in
"start"){
        for i in hadoop162 hadoop163
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /opt/module/flume-1.9.0/bin/flume-ng agent -n a1 -c /opt/module/flume-1.9.0/conf/ -f /opt/module/flume-1.9.0/job/file_to_kafka.conf >/dev/null 2>&1 &"
        done
};; 
"stop"){
        for i in hadoop162 hadoop163
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file_to_kafka | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "
        done

};;

*)
echo '你启动的姿势不对: '
echo 'flume.sh start 启动日志数据采集'
echo 'flume.sh stop  停止日志数据采集'

;;
esac

