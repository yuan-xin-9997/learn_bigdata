#!/bin/bash  
# 远程启动应用程序脚本
if [ $# -lt 5 ]
then
	echo "usages: $0 Ctr Sys Srv SrvNo Args"
	exit 1
fi

source ${HOME}/shell/setenv.sh
Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

BasePath=${HOME}/${Sys}
cd $BasePath
find . -name "*.ini" -or -name "*.sh" -or -name "*.xml" -or -name "*.yml" -or -name "*.cfg" |xargs dos2unix 2>/dev/null
find . -name "*.sh" |xargs chmod +x 2>/dev/null

sh start.sh $Ctr $Sys $Srv $SrvNo $Args  2>/dev/null