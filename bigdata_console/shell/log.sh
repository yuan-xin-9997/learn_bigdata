#!/bin/bash  
# bigdata console 日志输出脚本

if [ $# -le 1 ]
then
	echo "usages: $0 [-l error|warn|info|critical|debug] -m msg -t all|log|console"
	exit 1
fi

level=info
message=""
output_type=all

TIME=`date +%Y%m%d-%H%M%S`
LogFileDir=`getcfg.sh LogFileDir`
LogFileName=`getcfg.sh LogFileName`
HOSTNAME=`uname -a|awk '{print $2}'`


while getopts m:f:l:i:p:h:t:e:a:v: options
do
        case $options in
        m) message=$OPTARG 
                cnt=`expr $cnt + 2 ` ;;
        l) level=$OPTARG 
                cnt=`expr $cnt + 2 ` ;;
        t) output_type=$OPTARG 
                cnt=`expr $cnt + 2 ` ;;
        \?) echo [error] unsupported operation ${options}
                exit 1;;
       esac
done

LogFileToWrite=${LogFileDir}/${LogFileName}-${TIME}.log
if [ ! -e "${LogFileToWrite}" ];then
	# 如果文件不存在，先逐级创建其目录  
    mkdir -p "$(dirname "$LogFileToWrite")"  
      
    # 创建文件  
    touch "$LogFileToWrite"  
fi

if [ "${output_type}" == "all" ];then
	echo "${TIME} ${HOSTNAME} $level $message" >> ${LogFileToWrite}
	echo "${TIME} ${HOSTNAME} $level $message"
elif [ "${output_type}" == "log" ];then
	echo "${TIME} ${HOSTNAME} $level $message" >> ${LogFileToWrite}
elif [ "${output_type}" == "console" ];then
	echo "${TIME} ${HOSTNAME} $level $message"
fi