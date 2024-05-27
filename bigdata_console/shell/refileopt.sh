#!/bin/bash  
# 远程文件拷贝程序

source ~/shell/setenv.sh

if [ $# -lt 4 ]
then
    echo "usage: $0 local2remote|remote2local Remote sourceDir destinationDir"
    exit 1
fi

transferMode=$1
Remote=$2
sourceDir=$3
destinationDir$4
SOURCE_USER=`whoami`
DEST_USER=`whoami`

  
# 使用rsync进行同步  
# 注意：你需要确保SSH密钥已经设置，以便无密码登录
if [ "${transferMode}" == "local2remote" ];then
	rsync -avz --progress --delete "${sourceDir}" "${DEST_USER}@${Remote}:${destinationDir}/"
	result=$?
elif [ "${transferMode}" == "remote2local" ];then
	rsync -avz --progress --delete "${SOURCE_USER}@${Remote}:${sourceDir}" "${destinationDir}/" 
	result=$?
else
	echo "Error: unsupported transfer mode [${transferMode}]"
	exit 1
fi

# 检查rsync的退出状态，如果非0则输出错误并退出  
if [ ${result} -ne 0 ]; then  
    echo "Error occurred during rsync."  
    exit 1  
fi  

echo "Synchronization completed successfully."