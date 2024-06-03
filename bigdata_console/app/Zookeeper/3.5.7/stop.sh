#!/bin/bash  
# Hadoop系统停止脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "$Srv" == "NameNode" ];then
    echo "Stopping NameNode"
	hdfs --daemon stop namenode
elif [ "$Srv" == "DataNode" ];then
    echo "Stopping DataNode"
	hdfs --daemon stop datanode
elif [ "$Srv" == "SecondaryNameNode" ];then
    echo "Stopping SecondaryNameNode"
	hdfs --daemon stop secondarynamenode	
elif [ "$Srv" == "ResourceManager" ];then
    echo "Stopping ResourceManager"
	hdfs --daemon stop resourcemanager
elif [ "$Srv" == "NodeManager" ];then
    echo "Stopping NodeManager"
	hdfs --daemon stop nodemanager
elif [ "$Srv" == "historyserver" ];then
    echo "Stopping historyserver"
	mapred --daemon stop historyserver
fi

if [ "$Srv" == "NameNode" ];then
	echo "Stopping NameNode"
	hadoop-daemon.sh stop namenode
elif [ "$Srv" == "DataNode" ];then
    echo "Stopping DataNode"
	hadoop-daemon.sh stop datanode
elif [ "$Srv" == "SecondaryNameNode" ];then
    echo "Stopping SecondaryNameNode"
	hadoop-daemon.sh stop secondarynamenode
elif [ "$Srv" == "ResourceManager" ];then
    echo "Stopping ResourceManager"
	yarn-daemon.sh stop resourcemanager
elif [ "$Srv" == "NodeManager" ];then
    echo "Stopping NodeManager"
	yarn-daemon.sh stop nodemanager
elif [ "$Srv" == "historyserver" ];then
    echo "Stopping historyserver"
	yarn-daemon.sh stop historyserver
fi
