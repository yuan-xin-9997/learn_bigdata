#!/bin/sh
# 设置环境变量程序

SHELLPATH=$HOME/shell
PATH=$PATH:$HOME/shell

# 放开文件和core文件大小限制
ulimit -c unlimited
ulimit  unlimited
umask 027

# 设置list文件
ServiceListFile=`getcfg.sh ServiceListFile`
SHELLPATH=`getcfg.sh SHELLPATH`

Sys=$1
if [ "$Sys" == "Hadoop" ];then
	BasePath=${HOME}/${Sys}
	JAVA_HOME=${HOME}/`getcfg.sh ${Sys}_Java`
	PATH=$PATH:$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/bin/:$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/sbin/:${JAVA_HOME}/bin
elif [ "Zookeeper" == "${Sys}" ];then
    BasePath=${HOME}/${Sys}
	JAVA_HOME=${HOME}/`getcfg.sh ${Sys}_Java`
	export PATH=$PATH:${BasePath}/apache-zookeeper-`getcfg.sh ${Sys}_Version`-bin/bin/:${JAVA_HOME}/bin
else
    JAVA_HOME=`getcfg.sh JAVA_HOME`
    PATH=$PATH:${JAVA_HOME}/bin
fi

# 提升变量作用域
export SHELLPATH
export PATH
export JAVA_HOME