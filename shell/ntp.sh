#!/bin/sh
for host in `cat $HADOOP_HOME/etc/hadoop/workers`
	do
                echo $host
		ssh $host "sudo ntpdate ntp.aliyun.com"
	done
