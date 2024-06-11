#!/bin/bash

if (($#==0)); then
  echo -e "请输入参数：\n start  启动redis;\n stop  停止redis;\n" && exit
fi

case $1 in
  "start")
        redis-server ~/redis/redis.conf
      ;;
  "stop")
        redis-cli -p 6379 -a 123456 shutdown
      ;;
  "show"|"status")
       redis-cli -p 6379 -a 123456 ping
     ;;
    *)
        echo -e "---------- 请输入正确的参数 ----------\n"
        echo -e "请输入参数：\n start  启动redis;\n stop  停止redis;\n" && exit
      ;;
esac