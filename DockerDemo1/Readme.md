# 注意

## 由于 git 不支持大文件, 所以一些安装包 push 不上去.请在下面的地址下载完整资料:
链接: https://pan.baidu.com/s/1c3GvHUxai5d-rozNE3BiaA  密码: i183

## 目录说明
- 不要更改`docker_bigdata`及子目录的目录结构
- 进入到`docker_bigdata`目录下再执行脚本

## 镜像构建说明
- build_all.sh 可以构建需要用到的所有镜像, 运行的时候, 根据自己的需要选择需要构建的镜像
- build_all.sh 最后会创建 3 个容器 hadoop162, hadoop163, hadoop164

## 容器启动说明
- contains.sh 启动集群容器
- 启动的时候会自动根据当前宿主机的 ip 进行配置
- 3 个容器的 ip 分别是 xxx.162, xxx.163, xxx.164
- xxx 是根据当前宿主的 ip 自动获取的
- 内置了root和atguigu用户, 密码均为aaaaaa
- 第一次启动容器的时候自动配置虚拟机到容器的免密登录
- 对容器内需要初始化的各个集群完成了初始化

## 特别说明

- 容器一旦创建成功之后, 以后使用的时候启动即可
- 启动 hadoop:  hadoop.sh start
- 启动 kafka:  kafka.sh start
- 启动 zookeeper:  zk start
- 启动 hbase:  start-hbase.sh



