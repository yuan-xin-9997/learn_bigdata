#!/bin/bash
# utils工具

if [ $# -lt 1 ]; then
    echo "Usage: $0 Ctr Sys Srv SrvNo Args"
    exit 1
fi

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

# source ${HOME}/shell/setenv.sh $Sys
BasePath=${HOME}/${Sys}

# 获取Sys对应的家目录
getHomePath() {
    Ctr=$1
    Sys=$2
    Srv=$3
    SrvNo=$4
    Args=$5
    if [ $Sys == "Hadoop" ]; then
        HomePath=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/
    elif [ $Sys == "Hive" ]; then
        HomePath=""
    elif [ $Sys == "HBase" ]; then
        HomePath=$BasePath/hbase-`getcfg.sh ${Sys}_Version`/
    elif [ $Sys == "Zookeeper" ]; then
        HomePath=${BasePath}/apache-zookeeper-`getcfg.sh ${Sys}_Version`-bin/
    elif [ $Sys == "MySQL" ]; then
        dir_path=$(find "${BasePath}" -maxdepth 1 -type d -name "mysql-`getcfg.sh ${Sys}_Version`*" -print -quit)
        if [ -n "${dir_path}" ];then
            HomePath=${dir_path}
        else
            echo "[error] MySQL安装失败，未找到安装目录"
            exit 1
        fi
    else
        echo "[error] 未知系统：${Sys}"
        exit 1
    fi
    echo ${HomePath}
}

# todo 获取Sys对应的数据存储目录
getDataPath(){
    Ctr=$1
    Sys=$2
    Srv=$3
    SrvNo=$4
    Args=$5

}