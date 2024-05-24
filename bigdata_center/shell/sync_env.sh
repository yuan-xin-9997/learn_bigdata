#!/bin/bash

for host in `cat $HADOOP_HOME/etc/hadoop/workers`
do
	echo "=============$host============="
      	sudo scp /etc/profile.d/my_env.sh $host:/etc/profile.d/my_env.sh
done
