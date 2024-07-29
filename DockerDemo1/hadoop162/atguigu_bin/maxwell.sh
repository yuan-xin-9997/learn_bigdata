#!/bin/bash
maxwell_home=/opt/module/maxwell-1.27.1
case $1 in
    start)
        echo "========== $host 启动maxwell ========="
        source /etc/profile
        $maxwell_home/bin/maxwell --config $maxwell_home/config.properties --daemon 
       ;;

    stop)
        echo "========== $host停止 maxwell ========="
        source /etc/profile
        jps | awk '/Maxwell/ {print $1}' | xargs kill
        ;;

    *)
        echo "你启动的姿势不对"
        echo "  maxwell.sh start   启动canal集群"
        echo "  maxwell.sh stop    停止canal集群"

    ;;
esac
