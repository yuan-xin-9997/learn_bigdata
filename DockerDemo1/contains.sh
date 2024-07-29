#!/bin/bash
hosts=(162 163 164)
case $1 in
  "start")
    br0=$(ifconfig | grep ^br0)
    if [ -z "$br0" ]; then
        # 获取 eth0 或者 ens33 的 ip
        if [ -n "$(ifconfig | grep eth0)" ]; then
            ip=$(ifconfig eth0 | awk '/inet / { print $2 }')
            device=eth0
		elif [ -n "$(ifconfig | grep enp0s25)" ]; then
            ip=$(ifconfig enp0s25 | awk '/inet / { print $2 }')
            device=enp0s25
        else
            ip=$(ifconfig ens33 | awk '/inet / { print $2 }')
            device=ens33
        fi
        gateway=$(ip route show | awk '/default/ {print $3}' | awk 'NR==1') 
        echo ""
		pre_ip=$(ifconfig $device | awk '/inet / { print $2 }'|awk -F '.' '{print $1"."$2"."$3}')
		echo "配置hosts"
		sudo sh -c "echo '127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4' > /etc/hosts"
		sudo sh -c "echo '::1         localhost localhost.localdomain localhost6 localhost6.localdomain6' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.${hosts[0]} hadoop${hosts[0]}' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.${hosts[1]} hadoop${hosts[1]}' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.${hosts[2]} hadoop${hosts[2]}' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.102 hadoop102' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.103 hadoop103' >> /etc/hosts"
		sudo sh -c "echo '${pre_ip}.104 hadoop104' >> /etc/hosts"
		
		
        echo ""
        echo "开始搭建网桥...."
        sudo brctl addbr br0; \
        sudo ip link set dev br0 up; \
        sudo ip addr del "$ip"/24 dev $device; \
        sudo ip addr add "$ip"/24 dev br0 ; \
        sudo brctl addif br0 $device ; \
        sudo ip route add default via "$gateway" dev br0
        echo "网桥 br0 搭建成功"
    else
        echo "网桥已经搭建, 不需要重新搭建...."
    fi
	
    echo ""
    echo "开始启动容器....."
    for contain in ${hosts[*]} ; do
        echo "===== 启动容器: hadoop$contain      ======"
        docker start hadoop"$contain"
        echo "===== 容器: hadoop$contain 启动成功  ======"
        echo "++++++++++++++++++++++++++++++++++++"
    done

    echo "3 秒之后给容器配置 ip ......"
    sleep 3
    echo "开始给容器配置 ip ......"
    # ip前 3 个字段
    pre_ip=$(ifconfig br0 | awk '/inet / { print $2 }'|awk -F '.' '{print $1"."$2"."$3}')
    gateway=$(ip route show | awk '/default/ {print $3}' | awk 'NR==1') 
    sudo pipework  br0 hadoop"${hosts[0]}" "$pre_ip"."${hosts[0]}"/24@"$gateway"
    sudo pipework  br0 hadoop"${hosts[1]}" "$pre_ip"."${hosts[1]}"/24@"$gateway"
    sudo pipework  br0 hadoop"${hosts[2]}" "$pre_ip"."${hosts[2]}"/24@"$gateway"
    echo "完成容器 ip 配置 ......"
	
    if [ ! -f "/etc/already" ]; then
        echo "3 秒之后设置虚拟机到容器的免密登录"
        sleep 3
        echo "开始设置虚拟机到容器的免密登录"
        rm -rf ~/.ssh/known_hosts
        for host in hadoop162 hadoop163 hadoop164 ; do
            sshpass -p aaaaaa ssh-copy-id  -o StrictHostKeyChecking=no atguigu@$host >/dev/null 2>&1
        done
        echo "完成设置虚拟机到容器的免密登录"
        sudo touch "/etc/already" 
        echo "第一次启动容器, 需要对容器做一些初始化操作, 请耐心等待..."
        ssh atguigu@hadoop162 "~/bin/ssh_auto_copy.sh"
        echo "初始化完成!!!"
        echo "修改默认时区为东八区"
	
        ssh atguigu@hadoop162 "~/bin/xcall sudo cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime"
    fi
   # 临时增加
	
  ;;
  "stop")
    echo "开始关闭容器....."
    for contain in ${hosts[*]} ; do
        echo "===== 关闭容器: hadoop$contain     ======"
        docker stop hadoop"$contain"
    done
  ;;
  *)
    echo "你启动的姿势不对, 换个姿势再来"
    echo " contains.sh  start 启动所有容器"
    echo " contains.sh  stop 关闭所有容器"
  ;;
esac

