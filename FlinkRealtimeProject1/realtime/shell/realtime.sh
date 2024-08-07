#!/bin/bash
# 2024年8月7日20:28:38 往Flink集群中提交实时任务

flink=/opt/module/flink-1.13.6/bin/flink
jar=/opt/gmall/realtime-1.0-SNAPSHOT.jar

# 存储要启动的App的主类
apps=(
com.atguigu.realtime.app.dim.Ods2Dim
)

# 获取Flink正在运行的App的Name
running_apps=`$flink list 2>/dev/null| awk '/RUNNING/{print $(NF-1)}'`

for app in ${apps[*]};do
    app_name=`echo $app|awk -F. '{print \$NF}'`
    if [[ "${running_apps[@]}" =~ "$app_name" ]];then
        echo "$app_name is running"
    else
        echo "starting $app_name"
        $flink run -d -c $app $jar
    fi
done