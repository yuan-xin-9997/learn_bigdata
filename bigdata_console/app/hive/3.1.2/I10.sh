#!/bin/bash
# hive系统安装脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh $Sys
ServiceListFile=`getcfg.sh ServiceListFile`

# 先加载所有Hadoop参数
Hadoop_All_Args=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys -n -ocols Args`
for arg in $Hadoop_All_Args
do
 eval "$arg"
done

# 再加载当前服务的参数，如果当前服务参数与全局服务参数有重名，则使用当前服务的参数
eval "$Args"

# 解压压缩包
BasePath=${HOME}/${Sys}
cd $BasePath/install
tar -xzvf hadoop-`getcfg.sh ${Sys}_Version`.tar.gz -C ${BasePath} >/dev/null

# 拷贝脚本和配置文件到指定目录
cp -r $BasePath/install/start.sh $BasePath/
cp -r $BasePath/install/stop.sh $BasePath/
cp -r $BasePath/install/show.sh $BasePath/
# cp -r $BasePath/install/remove.sh $BasePath/
cp -r $BasePath/install/*.xml $BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/

# 修改配置文件
CoreSiteXML=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/core-site.xml
HdfsSiteXML=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/hdfs-site.xml
YarnSiteXML=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/yarn-site.xml
MapredSiteXML=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/mapred-site.xml
WorkerFile=$BasePath/hadoop-`getcfg.sh ${Sys}_Version`/etc/hadoop/workers
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
# 修改core-site.xml中的superUser用户，如果service.list配置了，则使用配置用户，否则使用whoami获取的用户
if [ -z "$SuperUser" ];then
    superUser=`whoami`
fi
# 获取当前服务器内存大小
total_memory_mb=$(free -m | awk 'NR==2{print $2}')
# NodeManager使用内存数，如果service.list配置了，则使用配置参数，否则使用当前机器的内存/4
if [ -z "$NodeManagerUseMemoryMB" ];then
    NodeManagerUseMemoryMB=`expr $total_memory_mb / 4`
fi
# 容器最小使用内存数，如果service.list配置了，则使用配置参数，否则使用当前机器的内存/16
if [ -z "$NMContainerMinMemoryMB" ];then
    NMContainerMinMemoryMB=`expr $total_memory_mb / 16`
fi
# 容器最大使用内存数，如果service.list配置了，则使用配置参数，否则使用当前机器的内存/4
if [ -z "$NMContainerMaxMemoryMB" ];then
    NMContainerMaxMemoryMB=`expr $total_memory_mb / 4`
fi

# 修改XML配置文件
sed -i "s/NameNodeHost:NameNodePort/${NameNodeHost}:${NameNodePort}/g"  $CoreSiteXML
sed -i "s|DataDir|${DataDir}|g"  $CoreSiteXML
sed -i "s/HttpStaticUser/${HttpStaticUser}/g"  $CoreSiteXML
sed -i "s/NameNodeHost:NameNodeWebPort/${NameNodeHost}:${NameNodeWebPort}/g"  $HdfsSiteXML
sed -i "s/SecondaryNameNodeHost:SecondaryNameNodeWebPort/${SecondaryNameNodeHost}:${SecondaryNameNodeWebPort}/g"  $HdfsSiteXML
sed -i "s/ResourceManagerHost/${ResourceManagerHost}/g"  $YarnSiteXML
sed -i "s/historyserverHost:historyserverPort/${historyserverHost}:${historyserverPort}/g"  $MapredSiteXML
sed -i "s/historyserverHost:historyserverWebPort/${historyserverHost}:${historyserverWebPort}/g"  $MapredSiteXML
sed -i "s/historyserverHost:historyserverWebPort/${historyserverHost}:${historyserverWebPort}/g"  $YarnSiteXML
sed -i "s/superUser/${superUser}/g"  $CoreSiteXML
sed -i "s/NodeManagerUseMemoryMB/${NodeManagerUseMemoryMB}/g"  $YarnSiteXML
sed -i "s/NMContainerMinMemoryMB/${NMContainerMinMemoryMB}/g"  $YarnSiteXML
sed -i "s/NMContainerMaxMemoryMB/${NMContainerMaxMemoryMB}/g"  $YarnSiteXML

# 将当前节点信息写入到workers文件中
CurrentHost=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys |findline.sh -icol Service=$Srv |findline.sh -icol ServiceNo=$SrvNo -n -ocols IP`
if [ ! -e "$WorkerFile" ]; then
    touch "$WorkerFile"
    echo "创建文件 $WorkerFile"
	echo $CurrentHost >> $WorkerFile
else
	flag=`grep -r -F $CurrentHost $WorkerFile`
	if [ $? -eq 1 ];then
		echo $CurrentHost >> $WorkerFile
	fi
fi

# 将service.list中Hadoop部署的节点信息写入到workers文件中
workers=`cat $ServiceListFile|findline.sh -icol Center=$Ctr|findline.sh -icol System=$Sys -n -ocols IP | sort -u`
for work in ${workers}
do
    flag=`grep -r -F $work $WorkerFile`
	if [ $? -eq 1 ];then
		echo $work >> $WorkerFile
	fi
done

# 创建Data目录
mkdir -p ${DataDir}
