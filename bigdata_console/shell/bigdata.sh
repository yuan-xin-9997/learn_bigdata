#/bin/bash
# bigdata console Entry Point

ServiceListFile=`getcfg.sh ServiceListFile`
SHELLPATH=`getcfg.sh SHELLPATH`
LocalRuntimeEnvDir=`getcfg.sh LocalRuntimeEnvDir`

if [ $# -le 1 ]
then
	echo "usages: bigdata.sh makeBase|createService|copyApp|startService|stopService|show|run [-ctr Ctr -sys Sys -srv Srv -srvno SrvNo -cmd CMD -args Args]"
	exit 1
fi

makeBase(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	# 发送脚本
	${SHELLPATH}/recmdopt.sh $Remote "mkdir -p shell"
	${SHELLPATH}/refileopt.sh local2remote $Remote "$SHELLPATH/" "$SHELLPATH"
	${SHELLPATH}/recmdopt.sh $Remote "dos2unix ${SHELLPATH}/*.* $SHELLPATH/console/*.* 2>/dev/null 1>/dev/null  ; chmod +x ${SHELLPATH}/*.sh > /dev/null"
	# 发送JDK
	JDK_Version=`getcfg.sh ${Sys}_Java`
	if [ -z "${JDK_Version}" ];then
		log.sh -l warn -m "cannot obtain JDK Version of ${Sys} in sh.cfg, please check!" -t all
		JDK_Version=`getcfg.sh JAVA_HOME`
		if [ -z "${JDK_Version}" ];then
		    log.sh -l error -m "cannot obtain JAVA_HOME in sh.cfg, please check!" -t all
		    exit 1
		fi
	fi
	${SHELLPATH}/recmdopt.sh $Remote "mkdir -p ${JDK_Version}"
	${SHELLPATH}/refileopt.sh local2remote $Remote "${LocalRuntimeEnvDir}/${JDK_Version}/" "${JDK_Version}"
}

delBase(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	# 删除shell
	${SHELLPATH}/recmdopt.sh $Remote "rm -rf  ${HOME}/shell"
	# 删除JDK
	JDK_Version=`getcfg.sh ${Sys}_Java`
	if [ -z "${JDK_Version}" ];then
		log.sh -l warn -m "cannot obtain JDK Version of ${Sys} in sh.cfg, please check!" -t all
		JDK_Version=`getcfg.sh JAVA_HOME`
		if [ -z "${JDK_Version}" ];then
		    log.sh -l error -m "cannot obtain JAVA_HOME in sh.cfg, please check!" -t all
		fi
	fi
	if [ ! -z "${JDK_Version}"];then
    	${SHELLPATH}/recmdopt.sh $Remote "rm -rf  ${HOME}/${JDK_Version}"
	fi
	# 删除系统目录
	${SHELLPATH}/recmdopt.sh $Remote "rm -rf  ${HOME}/${Sys}"
}

createService(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	Sys_Version=`getcfg.sh ${Sys}_Version`
	${SHELLPATH}/recmdopt.sh $Remote "rm -rf ${HOME}/${Sys}/*"
	${SHELLPATH}/recmdopt.sh $Remote "mkdir -p ${HOME}/${Sys}/; cd ${HOME}/${Sys}/; mkdir install logs"
}

removeService(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	Sys_Version=`getcfg.sh ${Sys}_Version`
	# ${SHELLPATH}/recmdopt.sh $Remote "rm -rf ${HOME}/${Sys}/*"
	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/remove.sh $Ctr $Sys $Srv $SrvNo $Args"
}

copyApp(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	Sys_Version=`getcfg.sh ${Sys}_Version`
	LoacalAppDir=`getcfg.sh LoacalAppDir`
	${SHELLPATH}/recmdopt.sh $Remote "rm -f ${HOME}/${Sys}/install/*"
	${SHELLPATH}/refileopt.sh local2remote $Remote ${LoacalAppDir}/$Sys/${Sys_Version}/ ${HOME}/${Sys}/install
	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/install.sh $Ctr $Sys $Srv $SrvNo $Args"
}

startService(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/start.sh $Ctr $Sys $Srv $SrvNo $Args"
}

stopService(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/stop.sh $Ctr $Sys $Srv $SrvNo $Args"
}

show(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/show.sh $Ctr $Sys $Srv $SrvNo $Args"
}

run(){
	Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	BasePath=${HOME}/${Sys}
	if [ -n "${remote_CMD}" ];then
    	${SHELLPATH}/recmdopt.sh $Remote "${SHELLPATH}/run.sh $Ctr $Sys $Srv $SrvNo ${remote_CMD}"
    else
        log.sh -l warn -m "you input command is empty!" -t console
    fi
}

cleanLog(){
    Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	${SHELLPATH}/recmdopt.sh $Remote "rm -rf ${HOME}/${Sys}/logs/*"
}

cleanData(){
    Remote=$1
	Ctr=$2
	Sys=$3
	Srv=$4
	SrvNo=$5
	Args=$6
	${SHELLPATH}/recmdopt.sh $Remote "rm -rf ${HOME}/${Sys}/data/*"  # todo
}


callImpl()
{		
	No=$1
	Func=$2
	Ctr=$3
	Sys=$4
	Srv=$5
	SrvNo=$6
	Remote=$7
	shift 7
	Args=$*
	
	if [ $Func != "show" ]
	then
		printf "No.%02d %6s %12s %18s %2s [%s]:\n" $No $Ctr $Sys $Srv $SrvNo $Remote
		$Func $Remote $Ctr $Sys $Srv $SrvNo $Args
	else
		#printf "%2d  " $No
		printf "No.%02d %6s %12s %18s %2s [%s]:\n" $No $Ctr $Sys $Srv $SrvNo $Remote
		$Func $Remote $Ctr $Sys $Srv $SrvNo $Args
	fi
}


Date=`date +%Y%m%d`
TmpListFile=/tmp/srv.list.$$.`whoami`.$Date
TmpFile=/tmp/srv.list.tmp.$$.`whoami`.$Date
cat $ServiceListFile > $TmpListFile
func=$1
shift 1
while [ $# -gt 1 ]
do
	cp $TmpListFile $TmpFile
	case $1 in
	-ctr)
			for thectr in ${ctrs}
			do
					cat $TmpFile |findline.sh -icol Center=$thectr -g >> $TmpListFile
			done
			shift 2
			;;
	-ictr)
			for thectr in ${ctrs}
			do
					cat $TmpFile |findline.sh -iicol Center=$thectr -g >> $TmpListFile
			done
			shift 2
			;;	
	-sys)
			cat $TmpFile |findline.sh -icol System=$2 -g > $TmpListFile
			shift 2
			;;
	-isys)
			cat $TmpFile |findline.sh -iicol System=$2 -g > $TmpListFile
			shift 2
			;;
	-srv)
			cat $TmpFile |findline.sh -icol Service=$2 -g > $TmpListFile
			shift 2
			;;
	-isrv)
			cat $TmpFile |findline.sh -iicol Service=$2 -g > $TmpListFile
			shift 2
			;;
	-srvno)
			cat $TmpFile |findline.sh -icol ServiceNo=$2 -g > $TmpListFile
			shift 2
			;;
	-isrvno)
			cat $TmpFile |findline.sh -iicol ServiceNo=$2 -g > $TmpListFile
			shift 2
			;;
	-cmd)
			remote_CMD=$2
			shift 2
			;;
	-arg)
			#将/替换为空格
			Args=$(echo $2|sed 's/\// /g')
			shift 2
			input_args_flag=true
			;;
	*)
		   #usage callall
		   # shift $#
		  echo "parameter illegal!"
		  exit 1
		   
	esac
done

# 删除TmpListFile文件标题头
#cnt_without_head=`sed '1d' $TmpListFile`
#echo ${cnt_without_head} > $TmpListFile
TmpRightListFile=/tmp/right.srv.list.tmp.$$.`whoami`.$Date
sed '1d' $TmpListFile > $TmpRightListFile
cntexpr=`wc -l $TmpRightListFile |awk '{print $1}'`
cnt=`expr $cntexpr`
i=0
j=1

while [ $i -lt $cnt ]
do			
	i=`expr $i + 1 `
	result=0
	if [ "$Args" == "" ]
	then
		cmdline=`head -$i $TmpRightListFile|tail -1`	
		Args=""  # todo: 从ServiceListFile的Args列读取输入参数
		callImpl $j $func $cmdline $Args
		result=$?
	else
		cmdline=`head -$i $TmpRightListFile|tail -1|awk '{print $1,$2,$3,$4,$5,$6,$7 }'` 
		callImpl $j $func $cmdline $Args
		result=$?
	fi
	if [ $result -eq 0 ]
		then 
			j=`expr $j + 1 `
	fi 
done