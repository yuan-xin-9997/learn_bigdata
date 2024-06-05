#!/bin/bash
# MySQL系统安装脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys
ServiceListFile=`getcfg.sh ServiceListFile`

# 先加载所有MySQL参数
MySQL_All_Args=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys -n -ocols Args`
for arg in $MySQL_All_Args
do
 eval "$arg"
done

# 再加载当前服务的参数，如果当前服务参数与全局服务参数有重名，则使用当前服务的参数
eval "$Args"

# 解压压缩包
BasePath=${HOME}/${Sys}
cd $BasePath/install
tar -xvf mysql-`getcfg.sh ${Sys}_Version`-*.tar -C ${BasePath} >/dev/null

# 拷贝脚本和配置文件到指定目录
cp -r $BasePath/install/start.sh $BasePath/
cp -r $BasePath/install/stop.sh $BasePath/
cp -r $BasePath/install/show.sh $BasePath/
cp -r $BasePath/install/remove.sh $BasePath/

#===========================================个性化配置==============================================
# 软件家目录
dir_path=$(find "${BasePath}" -maxdepth 1 -type d -name "mysql-`getcfg.sh ${Sys}_Version`*" -print -quit)
if [ -n "${dir_path}" ];then
    MySQL_HOME=${dir_path}
else
    echo "[error] MySQL安装失败，未找到安装目录"
    exit 1
fi

# 创建mysql用户和用户组
sudo groupadd mysql
sudo useradd -r -g mysql mysql

# 修改配置文件
MySQLDataPath=`getcfg.sh MySQLDataPath`
if [ -e "$MySQLDataPath"  ];then
    rm -rf $MySQLDataPath/*
else
    mkdir -p $MySQLDataPath
fi
sudo chown -R mysql:mysql $MySQLDataPath
if [ -z "$MySQLPort" ];then
	MySQLPort=3306
fi

MySQLLogError=`getcfg.sh MySQLLogError`
if [ -n "$MySQLLogError"  ];then
    MySQLLogError=${BasePath}/logs/error.log
fi
MySQLPidFile=`getcfg.sh MySQLPidFile`
if [ -n "$MySQLPidFile"  ];then
    MySQLPidFile=${BasePath}/logs/mysqld.pid
fi

MyCNF=${MySQL_HOME}/support-files/my-default.cnf
touch $MyCNF
cat > $MyCNF <<EOF
[mysqld]
sql_mode=NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES

basedir = ${MySQL_HOME}
datadir = ${MySQLDataPath}
port = ${MySQLPort}
socket = /tmp/mysql.sock
character-set-server=utf8

log-error = ${MySQLLogError}
pid-file = ${MySQLPidFile}
EOF

cp -f $MyCNF /etc/my.cnf


# 初始化数据库
install_log=${BasePath}/install/install.log
echo "=============${MySQL_HOME}/bin/mysqld --initialize --user=mysql --basedir=${BasePath} --datadir=${MySQLDataPath}==========" >${install_log}
${MySQL_HOME}/bin/mysqld --initialize --user=mysql --basedir=${BasePath} --datadir=${MySQLDataPath} >>${install_log} 2>&1
