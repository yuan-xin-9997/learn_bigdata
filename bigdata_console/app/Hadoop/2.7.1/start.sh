#!/bin/bash  
# Hadoop系统启动脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "Srv" == "NameNode" ];then
	hdfs --daemon start namenode
elif [ "Srv" == "DataNode" ];then
	hdfs --daemon start datanode
elif [ "Srv" == "SecondaryNameNode" ];then
	hdfs --daemon start secondarynamenode	
elif [ "Srv" == "ResourceManager" ];then
	hdfs --daemon start resourcemanager
elif [ "Srv" == "NodeManager" ];then
	hdfs --daemon start nodemanager
elif [ "Srv" == "historyserver" ];then
	mapred --daemon start historyserver
fi
