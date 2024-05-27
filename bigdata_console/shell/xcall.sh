#!/bin/bash

# 1. 判断参数个数
if [ $# == 0 ];then
  for host in hadoop102 hadoop103 hadoop104
    do
      echo ========== $host ==========
      ssh "$host" "jps;exit"
    done
fi

# 2. 如果有参数，就传参数
for host in hadoop102 hadoop103 hadoop104
    do
      echo ========== $host ==========
      ssh $host $*
    done
