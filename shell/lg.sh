#!/bin/bash
for host in hadoop102 hadoop103;do
	echo "==========$host==========="
	ssh $host "cd /opt/module/applog/; java -jar gmall2020-mock-log-2021-01-22.jar > /dev/null 2>&1 &"
done