#!/bin/bash
# 一键启动、停止 doris
source /etc/profile

# ！！此函数实现shell倒计时功能！！
function my_sleep(){
    for(( sec=$1;sec>=0;sec-- ))
    do
        echo -ne "\e[1;31m $sec $2\e[0m"
        echo -ne "\r"
        sleep 1
    done
    echo ''
}
case $1 in
    "start")
        if [ ! -f "/etc/doris" ]; then
            echo "第一次启动 doris 集群, 时间会久一些..."
            echo "在 hadoo162 启动 fe"
            /opt/module/doris-1.1.1/fe/bin/start_fe.sh --daemon
            my_sleep 20 "秒后在 hadoop163 和 hadoop164 上增加 fe 节点"  
            mysql -h hadoop162 -uroot -P 9030 -e " SET PASSWORD FOR 'root' = PASSWORD('aaaaaa'); \
                                                               ALTER SYSTEM ADD FOLLOWER 'hadoop163:9010'; \
                                                               ALTER SYSTEM ADD OBSERVER 'hadoop164:9010';
                                                             "
            for host in hadoop163 hadoop164; do
                echo  "在 $host 启动 fe"
                ssh $host "/opt/module/doris-1.1.1/fe/bin/start_fe.sh --helper hadoop162:9010 --daemon"
            done

            echo "添加 be 节点"
            mysql -h hadoop162 -uroot -P 9030 -paaaaaa -e " ALTER SYSTEM ADD BACKEND 'hadoop162:9050'; \
                                                                        ALTER SYSTEM ADD BACKEND 'hadoop163:9050'; \
                                                                        ALTER SYSTEM ADD BACKEND 'hadoop164:9050'; 
                                                                      "

            for host in hadoop162 hadoop163 hadoop164; do
                echo  "在 $host 启动 be"
                ssh $host "/opt/module/doris-1.1.1/be/bin/start_be.sh --daemon"
            done
            sudo touch /etc/doris
        else 
            echo "不是第一次启动 doris 集群, 正常启动..."
            for host in hadoop162 hadoop163 hadoop164 ; do
                echo "========== 在 $host 上启动 fe  ========="
                ssh $host "source /etc/profile; /opt/module/doris-1.1.1/fe/bin/start_fe.sh --daemon"
            done
            for host in hadoop162 hadoop163 hadoop164 ; do
                echo "========== 在 $host 上启动 be  ========="
                ssh $host "source /etc/profile; /opt/module/doris-1.1.1/be/bin/start_be.sh --daemon"
            done   
        fi
       ;;
    "stop")
            for host in hadoop162 hadoop163 hadoop164 ; do
                echo "========== 在 $host 上停止 fe  ========="
                ssh $host "source /etc/profile; /opt/module/doris-1.1.1/fe/bin/stop_fe.sh "
            done
            for host in hadoop162 hadoop163 hadoop164 ; do
                echo "========== 在 $host 上停止 be  ========="
                ssh $host "source /etc/profile; /opt/module/doris-1.1.1/be/bin/stop_be.sh "
            done

           ;;

    *)
        echo "你启动的姿势不对"
        echo "  start   启动doris集群"
        echo "  stop    停止stop集群"

    ;;
esac



