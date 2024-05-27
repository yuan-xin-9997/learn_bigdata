#!/bin/bash  
# Hadoop系统安装脚本

Ctr=$1
Sys=$2
Srv=$3
SrvNo=$4
Args=$5

source ${HOME}/shell/setenv.sh
ServiceListFile=`getcfg.sh ServiceListFile`

BasePath=${HOME}/${Sys}
cd $BasePath/install
# 解压压缩包
tar -xzvf hadoop-`getcfg.sh ${Sys}_Version`.tar.gz -C ${BasePath}

# 拷贝脚本和配置文件到指定目录
cp -r $BasePath/install/start.sh $BasePath/
cp -r $BasePath/install/stop.sh $BasePath/
cp -r $BasePath/install/show.sh $BasePath/
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

if [ "$Srv" == "NameNode" ];then
	sed -i "s/NameNodeHost/${NameNodeHost}/g"  $CoreSiteXML
	sed -i "s/NameNodePort/${NameNodePort}/g"  $CoreSiteXML
	sed -i "s/DataDir/${DataDir}/g"  $CoreSiteXML
	sed -i "s/HttpStaticUser/${HttpStaticUser}/g"  $CoreSiteXML
	sed -i "s/NameNodeHost/${NameNodeHost}/g"  $HdfsSiteXML
	sed -i "s/NameNodeWebPort/${NameNodeWebPort}/g"  $HdfsSiteXML
fi
if [ "$Srv" == "SecondaryNameNode" ];then
	sed -i "s/SecondaryNameNodeHost/${SecondaryNameNodeHost}/g"  $HdfsSiteXML
	sed -i "s/SecondaryNameNodeWebPort/${SecondaryNameNodeWebPort}/g"  $HdfsSiteXML
fi
if [ "$Srv" == "ResourceManager" ];then
	sed -i "s/ResourceManagerHost/${ResourceManagerHost}/g"  $YarnSiteXML
fi
if [ "$Srv" == "historyserver" ];then
	sed -i "s/historyserverHost/${historyserverHost}/g"  $MapredSiteXML
	sed -i "s/historyserverPort/${historyserverPort}/g"  $MapredSiteXML
	sed -i "s/historyserverWebPort/${historyserverWebPort}/g"  $MapredSiteXML
fi
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

# 创建Data目录
mkdir -p ${DataDir}