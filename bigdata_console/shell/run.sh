#!/bin/bash  
# 在指定系统的语境下执行命令

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5
CMD=$6

source ${HOME}/shell/setenv.sh $Sys
$CMD

