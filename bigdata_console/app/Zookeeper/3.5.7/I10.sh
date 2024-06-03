#!/bin/bash
# Zookeeper系统安装脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys
ServiceListFile=`getcfg.sh ServiceListFile`

#Zookeeper_Args=`getcfg.sh Zookeeper_Args`
# 加载所有Zookeeper参数
Zookeeper_All_Args=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys -n -ocols Args`
for arg in $Zookeeper_All_Args
do
 eval "$arg"
done

BasePath=${HOME}/${Sys}
cd $BasePath/install
# 解压压缩包
tar -xzvf Zookeeper-`getcfg.sh ${Sys}_Version`.tar.gz -C ${BasePath} >/dev/null
tar -xzvf apache-zookeeper-`getcfg.sh ${Sys}_Version`-bin.tar.gz -C ${BasePath} >/dev/null

# 拷贝脚本和配置文件到指定目录
cp -r $BasePath/install/start.sh $BasePath/
cp -r $BasePath/install/stop.sh $BasePath/
cp -r $BasePath/install/show.sh $BasePath/
cp -r $BasePath/install/*.xml $BasePath/zookeeper-`getcfg.sh ${Sys}_Version`/

# 创建数据目录
zkDataPath=`getcfg.sh zkDataPath`
if [ -z "$zkDataPath" ];then
	NameNodePort=8020
fi
mkdir -p $BasePath/zkData/

# 修改配置文件
CoreSiteXML=$BasePath/Zookeeper-`getcfg.sh ${Sys}_Version`/etc/Zookeeper/core-site.xml
HdfsSiteXML=$BasePath/Zookeeper-`getcfg.sh ${Sys}_Version`/etc/Zookeeper/hdfs-site.xml
YarnSiteXML=$BasePath/Zookeeper-`getcfg.sh ${Sys}_Version`/etc/Zookeeper/yarn-site.xml
MapredSiteXML=$BasePath/Zookeeper-`getcfg.sh ${Sys}_Version`/etc/Zookeeper/mapred-site.xml
WorkerFile=$BasePath/Zookeeper-`getcfg.sh ${Sys}_Version`/etc/Zookeeper/workers
NameNodeHost=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys |findline.sh -icol Service=NameNode |findline.sh -icol ServiceNo=1 -n -ocols IP`
if [ -z "$NameNodePort" ];then
	NameNodePort=8020
fi
DataDir=$BasePath/data/
HttpStaticUser=`whoami`
if [ -z "$NameNodeWebPort" ];then
	NameNodeWebPort=9870
fi
SecondaryNameNodeHost=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys |findline.sh -icol Service=SecondaryNameNode |findline.sh -icol ServiceNo=1 -n -ocols IP`
if [ -z "$SecondaryNameNodeWebPort" ];then
	SecondaryNameNodeWebPort=9868
fi
ResourceManagerHost=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys |findline.sh -icol Service=ResourceManager |findline.sh -icol ServiceNo=1 -n -ocols IP`
historyserverHost=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys |findline.sh -icol Service=historyserver |findline.sh -icol ServiceNo=1 -n -ocols IP`
if [ -z "$historyserverPort" ];then
	historyserverPort=10020
fi
if [ -z "$historyserverWebPort" ];then
	historyserverWebPort=19888
fi