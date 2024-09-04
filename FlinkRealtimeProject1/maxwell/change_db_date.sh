#!/bin/bash
read -p "请输入你要模拟的日志日期, 格式: yyyy-mm-dd " day
sed -i "s/^mock.date.*\$/mock.date=$day/" /opt/software/mock/mock_db/application.properties
sed -i "s/^mock_date.*\$/mock_date=$day/" /opt/module/maxwell-1.27.1/config.properties
maxwell.sh stop
maxwell.sh start
