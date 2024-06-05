#!/bin/bash
# This script will remove the MySQL server and all data.

# 停止MySQL服务
sudo service mysql stop
sudo service mysql status

#  先卸载MariaDB
# 在CentOS中默认安装有MariaDB，是MySQL的一个分支，主要由开源社区维护。
# CentOS 7及以上版本已经不再使用MySQL数据库，而是使用MariaDB数据库。
# 如果直接安装MySQL，会和MariaDB的文件冲突。
# 因此，需要先卸载自带的MariaDB，再安装MySQL。

# 检查是否安装了MariaDB
sudo rpm -qa | grep mariadb
if [ ! $? -eq 0 ];then
    echo "Mariadb is not installed in this server."
else
    # 卸载MariaDB
    echo "sudo rpm -qa|sudo grep mariadb |sudo  xargs rpm -e --nodeps"
    sudo rpm -qa|sudo grep mariadb |sudo  xargs rpm -e --nodeps

    # 检查是否卸载MariaDB
    sudo rpm -qa | grep mariadb
    if [ ! $? -eq 0 ];then
        echo "Mariadb is not successful removed in this server."
    fi
fi

# 检查是否安装了MySQL
sudo rpm -qa | grep mysql
if [ ! $? -eq 0 ];then
    echo "MySQL is not installed in this server."
else
    # 卸载MySQL
    echo "sudo rpm -qa|sudo grep mysql |sudo xargs rpm -e --nodeps"
    sudo rpm -qa|sudo grep mysql |sudo xargs rpm -e --nodeps

    # 检查是否卸载MySQL
    rpm -qa | grep MySQL
    if [ ! $? -eq 0 ];then
        echo "MySQL is not successful removed in this server."
    fi

    # 删除MySQL的相关文件夹
    sudo rm -rf /var/lib/mysql
    sudo rm -rf /var/lib/mysql/mysql
    sudo rm -rf /usr/share/mysql
fi
