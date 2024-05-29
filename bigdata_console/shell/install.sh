#!/bin/bash  
# 远程安装应用程序脚本
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
cd $BasePath/install
find . -name "*.ini" -or -name "*.sh" -or -name "*.xml" -or -name "*.yml" -or -name "*.cfg" |xargs dos2unix 2>/dev/null
find . -name "*.sh" |xargs chmod +x 2>/dev/null

for runIt in `ls I[0-9][0-9]* 2>/dev/null|sort`
do
	echo install running $runIt ...
	sh $runIt $Ctr $Sys $Srv $SrvNo $Args  2>/dev/null
done