#!/bin/bash  
# Hadoop系统启动脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "$Srv" == "NameNode" ];then
	echo "Starting NameNode"
	hadoop-daemon.sh start namenode
elif [ "$Srv" == "DataNode" ];then
    echo "Starting DataNode"
	hadoop-daemon.sh start datanode
elif [ "$Srv" == "SecondaryNameNode" ];then
    echo "Starting SecondaryNameNode"
	hadoop-daemon.sh start secondarynamenode
elif [ "$Srv" == "ResourceManager" ];then
    echo "Starting ResourceManager"
	yarn-daemon.sh start resourcemanager
elif [ "$Srv" == "NodeManager" ];then
    echo "Starting NodeManager"
	yarn-daemon.sh start nodemanager
elif [ "$Srv" == "historyserver" ];then
    echo "Starting historyserver"
	yarn-daemon.sh start historyserver
fi
