#!/bin/bash
# 1. 判断是否存在参数
if [ $# == 0 ];then
  echo -e "请输入参数：\nstart   启动日志采集flume；\nstop   关闭日志采集flume；"&&exit
fi

FLUME_HOME=/opt/module/flume

# 2. 根据传入的参数执行命令
case $1 in
  "start"){
      # 3. 分别在hadoop102 hadoop103 上启动日志采集flume
      for host in hadoop102 hadoop103
        do
          echo "---------- 启动 $host 上的 日志采集flume ----------"
          ssh $host " nohup $FLUME_HOME/bin/flume-ng agent -n a1 -c $FLUME_HOME/conf/ -f $FLUME_HOME/job/flume-tailDir-kafka.conf -Dflume.root.logger=INFO,LOGFILE >$FLUME_HOME/logs/flume.log 2>&1 &"
        done
  };;
"stop"){
      # 4. 分别在hadoop102 hadoop103 上启动日志采集flume
      for host in hadoop102 hadoop103
        do
          echo "---------- 停止 $host 上的 日志采集flume ----------"
          flume_count=$(/home/atguigu/bin/xcall.sh jps -ml | grep flume-tailDir-kafka|wc -l);
          if [ $flume_count != 0 ];then
              ssh $host "ps -ef | grep flume-tailDir-kafka | grep -v grep | awk '{print \$2}' | xargs -n1 kill -9"
          else
              echo "$host 当前没有日志采集flume在运行"
          fi
        done
  };;
 "show"|"status"){
 for host in hadoop102 hadoop103
        do
          echo "---------- 显示 $host 上的 日志采集flume ----------"
          flume_count=$(/home/atguigu/bin/xcall.sh jps -ml | grep flume-tailDir-kafka|wc -l);
          if [ $flume_count != 0 ];then
              ssh $host "jps -ml | grep flume-tailDir-kafka"
          else
              echo "$host 当前没有日志采集flume在运行"
          fi
        done
 };;
esac