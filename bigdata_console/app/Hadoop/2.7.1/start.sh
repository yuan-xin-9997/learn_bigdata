#!/bin/bash  
# Hadoop系统启动脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "$Srv" == "NameNode" ];then
	hadoop-daemon.sh start namenode
elif [ "$Srv" == "DataNode" ];then
	hadoop-daemon.sh start datanode
elif [ "$Srv" == "SecondaryNameNode" ];then
	hadoop-daemon.sh start secondarynamenode
elif [ "$Srv" == "ResourceManager" ];then
	yarn-daemon.sh start resourcemanager
elif [ "$Srv" == "NodeManager" ];then
	yarn-daemon.sh start nodemanager
elif [ "$Srv" == "historyserver" ];then
	yarn-daemon.sh start historyserver
fi
