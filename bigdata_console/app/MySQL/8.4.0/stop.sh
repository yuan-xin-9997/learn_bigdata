#!/bin/bash  
# MySQL系统停止脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "$Srv" == "MySQL" ];then
	sudo systemctl stop mysql
	sudo systemctl status mysql
fi
