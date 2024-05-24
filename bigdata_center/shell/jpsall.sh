#!/bin/bash

#for host in hadoop102 hadoop103 hadoop104 hadoop105
for host in `cat $HADOOP_HOME/etc/hadoop/workers`
do
        echo =============== $host ===============
        ssh $host jps 
done
