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