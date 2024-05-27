#!/bin/bash  
# 远程命令执行程序
if [ $# -le 1 ]
then
	echo "usages: recmdopt.sh $RemoteIP $CMD"
	exit 1
fi

host=$1
CMD=$2

source ~/shell/setenv.sh
# echo "[$CMD] will be executed in host $host"
ssh "$host" "$CMD"
# echo "Done"