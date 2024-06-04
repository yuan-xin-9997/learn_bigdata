#!/bin/bash
# Zookeeper系统安装脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys
ServiceListFile=`getcfg.sh ServiceListFile`
BasePath=${HOME}/${Sys}

# 解压压缩包
cd $BasePath/install
tar -xzvf apache-zookeeper-`getcfg.sh ${Sys}_Version`-bin.tar.gz -C ${BasePath} >/dev/null

# 拷贝脚本和配置文件到指定目录
cp -r $BasePath/install/start.sh $BasePath/
cp -r $BasePath/install/stop.sh $BasePath/
cp -r $BasePath/install/show.sh $BasePath/

#===========================================个性化配置==============================================
#Zookeeper_Args=`getcfg.sh Zookeeper_Args`
# 加载所有Zookeeper参数
Zookeeper_All_Args=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys -n -ocols Args`
for arg in $Zookeeper_All_Args
do
 eval "$arg"
done

# 软件家目录
Zookeeper_Home=${BasePath}/apache-zookeeper-`getcfg.sh ${Sys}_Version`-bin/

# 创建数据目录
zkDataPath=`getcfg.sh zkDataPath`
if [ -z "$zkDataPath" ];then
	zkDataPath=$BasePath/zkData/
fi
mkdir -p $zkDataPath

# 创建myid文件
echo "$SrvNo" > $zkDataPath/myid

# 修改zoo.cfg配置文件
cd ${Zookeeper_Home}/conf
cp zoo_sample.cfg zoo.cfg
ZooCfg=${Zookeeper_Home}/conf/zoo.cfg

# 修改数据存储路径配置，default dataDir=/tmp/zookeeper
sed -i "s|dataDir=/tmp/zookeeper|dataDir=${zkDataPath}|g"  $ZooCfg
# 修改ZK客户端使用的端口
if [ -z "$clientPort" ];then
	clientPort=2181
fi
sed -i "s|clientPort=2181|clientPort=${clientPort}|g"  $ZooCfg

# 在zoo.cfg增加集群配置信息
# 获取所有ZK节点机
echo "" >> $ZooCfg
echo "#######################cluster##########################" >> $ZooCfg
awk -v Ctr=$Ctr -v Sys=$Sys '!/^#/ {if($1==Ctr && ($2==Sys))  {print $1,$2,$3,$4,$5,$6}}' ${ServiceListFile} | while read ctr sys srv srvno nodeip args
do
    # echo '====>' $ctr   $sys   $srv   $srvno   $nodeip  $args
    flag=`grep -r -F $nodeip $ZooCfg`
    if [ $? -eq 1 ];then
        for arg in ${args}
        do
            eval "$arg"
        done
        if [ -z "$ZkQuorumPort" ];then
	        ZkQuorumPort=2888
        fi
        if [ -z "$ZkElectionPort" ];then
	        ZkElectionPort=3888
        fi
        echo "server.${srvno}=${nodeip}:${ZkQuorumPort}:${ZkElectionPort}" >> $ZooCfg
    fi
done