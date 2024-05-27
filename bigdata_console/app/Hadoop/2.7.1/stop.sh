#!/bin/bash  
# Hadoop系统停止脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "Srv" == "NameNode" ];then
	hdfs --daemon stop namenode
elif [ "Srv" == "DataNode" ];then
	hdfs --daemon stop datanode
elif [ "Srv" == "SecondaryNameNode" ];then
	hdfs --daemon stop secondarynamenode	
elif [ "Srv" == "ResourceManager" ];then
	hdfs --daemon stop resourcemanager
elif [ "Srv" == "NodeManager" ];then
	hdfs --daemon stop nodemanager
elif [ "Srv" == "historyserver" ];then
	mapred --daemon stop historyserver
fi
