#!/bin/bash  
# Zookeeper系统启动脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "$Srv" == "Zookeeper" ];then
	echo "Starting ${Srv}"
	zkServer.sh start
fi
