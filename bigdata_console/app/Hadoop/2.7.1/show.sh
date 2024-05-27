#!/bin/bash  
# Hadoop系统状态显示脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys

if [ "Srv" == "NameNode" ];then
	jps |grep -i namenode
elif [ "Srv" == "DataNode" ];then
	jps |grep -i datanode
elif [ "Srv" == "SecondaryNameNode" ];then
	jps |grep -i secondarynamenode	
elif [ "Srv" == "ResourceManager" ];then
	jps |grep -i resourcemanager
elif [ "Srv" == "NodeManager" ];then
	jps |grep -i nodemanager
elif [ "Srv" == "historyserver" ];then
	jps |grep -i historyserver
fi
