#!/bin/sh
# 设置环境变量程序

SHELLPATH=$HOME/shell;export SHELLPATH
JAVA_HOME=`getcfg.sh JAVA_HOME`
PATH=$PATH:$HOME/shell:${JAVA_HOME}/bin;export PATH

#放开文件和core文件大小限制
ulimit -c unlimited
ulimit  unlimited
umask 027

# 设置list文件
ServiceListFile=`getcfg.sh ServiceListFile`
SHELLPATH=`getcfg.sh SHELLPATH`

Sys=$1

if [ "$Sys" == "Hadoop" ];then
	BasePath=${HOME}/${Sys}
	JAVA_HOME=`getcfg.sh ${Sys}_Java`
	export PATH=$PATH:$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/bin/:${JAVA_HOME}/bin
fi