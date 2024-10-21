dws
    汇总层

    轻度汇总
        小窗口级别的汇总

        0-10
        10-20
        20-30

    存储轻度汇总结果的介质，应该是支持SQL的数据库
        mysql   oltp数据库，不适合大数据量
        hive  查询延时太高，不适合于实时查询
        hbase+phoenix    适合
        clickhouse       适合
        doris            适合          ref https://doris.apache.org/zh-CN/docs/gettingStarted/what-is-new

        要求：能够实时查询、数据量要大

ads层
    根据具体时间进一步的汇总

    使用SQL语句
        select ... sum() .. from t where time >= ...  and time <= ...




10.1 流量域来源关键词粒度页面浏览各窗口汇总表
搜索记录  找到用户的搜索关键词，统计关键词在每个窗口内出现的次数

数据源
    页面日志
        "item" is not null  "item_type"="keyword" "last_page_id" = "search"

    "华为手机
    小米手机
    苹果手机"

    对关键词进行分词，IK分词器
    "华为手机"  "华为" "手机" ...

    开窗聚合，统计每个关键词的次数
        group window
        tvf  √
        over

    把统计结果写到doris




统计七日回流用户和当日独立用户数
数据源：
    日志数据
        页面日志
什么样的数据？
    当日独立用户数：今天登录的用户  计数
    七日回流用户：今天登录的用户中，上一次登录距离今天超过7天
具体的计算：
    肯定用到状态































