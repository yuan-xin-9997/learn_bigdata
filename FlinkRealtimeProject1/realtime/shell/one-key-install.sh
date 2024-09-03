#!/bin/bash
# 一键安装 Apache Doris 1.1.1

source /etc/profile
xcall " /opt/module/doris-1.1.1/be/bin/stop_fe.sh; \
        /opt/module/doris-1.1.1/fe/bin/stop_be.sh; \
		rm -rf /opt/module/doris-1.1.1; \
        sudo rm -rf /etc/doris
      "

tar -zxvf apache-doris-1.1.1-bin-x86.tar.gz -C /opt/module
mv /opt/module/apache-doris-1.1.1-bin-x86 /opt/module/doris-1.1.1
my_rsync /opt/module/doris-1.1.1
pre_ip=$(ifconfig eth1 | awk '/inet / { print $2 }'|awk -F '.' '{print $1"."$2"."$3}')


# 安装 fe
for host in 162 163 164; do
    echo "开始在 hadoop$host 配置 fe"
    ssh hadoop$host " mkdir /opt/module/doris-1.1.1/doris-meta; \
                      echo 'meta_dir = /opt/module/doris-1.1.1/doris-meta'  >> /opt/module/doris-1.1.1/fe/conf/fe.conf; \
                      echo 'priority_networks = ${pre_ip}.${host}/24'  >> /opt/module/doris-1.1.1/fe/conf/fe.conf; \
                      echo 'http_port = 7030'  >> /opt/module/doris-1.1.1/fe/conf/fe.conf 
                    "

done


# 安装 be
for host in 162 163 164; do
    echo  "开始在 hadoop$host 配置be"
    ssh hadoop$host " mkdir /opt/module/doris-1.1.1/doris-storage1; \
                      mkdir /opt/module/doris-1.1.1/doris-storage2.SSD; \
                      echo 'storage_root_path = /opt/module/doris-1.1.1/doris-storage1;/opt/module/doris-1.1.1/doris-storage2.SSD,10' >> /opt/module/doris-1.1.1/be/conf/be.conf; \
                      echo 'priority_networks = ${pre_ip}.${host}/24' >> /opt/module/doris-1.1.1/be/conf/be.conf; \
					  echo 'webserver_port = 7040' >> /opt/module/doris-1.1.1/be/conf/be.conf
                    "
done



