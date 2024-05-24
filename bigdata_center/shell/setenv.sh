#!/bin/sh
SHELLPATH=$HOME/shell;export SHELLPATH
PATH=$PATH:$HOME/shell;export PATH

#放开文件和core文件大小限制
ulimit -c unlimited
ulimit  unlimited
umask 027

# 设置list文件
ServiceListFile=`getcfg.sh ServiceListFile`