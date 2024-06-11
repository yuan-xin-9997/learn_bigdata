#!/bin/bash

if (($#==0)); then
  echo -e "请输入参数：\n start  启动kibana;\n stop  停止kibana;\n" && exit
fi

case $1 in
  "start")
        nohup /opt/module/kibana-7.8.0-linux-x86_64/bin/kibana &
      ;;
  "stop")
         ps -ef|grep -i kibana|grep -v grep|grep -v kibana.sh
         if [ $? -eq 0 ];then
           kill -9 $(ps -ef|grep -i kibana|grep -v grep|grep -v kibana.sh|awk '{print $2}')
           echo "kibana stopped"
         else
           echo "kibana is not running"
         fi
      ;;
  "show"|"status")
       ps -ef|grep -i kibana|grep -v grep|grep -v kibana.sh|awk '{print $2}'
     ;;
    *)
        echo -e "---------- 请输入正确的参数 ----------\n"
        echo -e "请输入参数：\n start  启动kibana;\n stop  停止kibana;\n" && exit
      ;;
esac