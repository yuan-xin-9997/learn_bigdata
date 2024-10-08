export JAVA_HOME=/opt/module/jdk1.8.0_212
export PATH=$JAVA_HOME/bin:$PATH

export HADOOP_HOME=/opt/module/hadoop-3.1.3
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

export ZOOKEEPER_HOME=/opt/module/zookeeper-3.5.7
export PATH=$ZOOKEEPER_HOME/bin:$PATH

export KAFKA_HOME=/opt/module/kafka-2.4.1
export PATH=$KAFKA_HOME/bin:$PATH

export HBASE_HOME=/opt/module/hbase-2.0.5
export PATH=$PATH:$HBASE_HOME/bin

#phoenix
export PHOENIX_HOME=/opt/module/phoenix-5.0.0
export PHOENIX_CLASSPATH=$PHOENIX_HOME
export PATH=$PATH:$PHOENIX_HOME/bin

export HIVE_HOME=/opt/module/hive-3.1.2
export PATH=$PATH:$HIVE_HOME/bin

export  LANG="en_US.UTF-8"

export HADOOP_CLASSPATH=`hadoop classpath`
