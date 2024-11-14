package com.atguigu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author: yuan.xin
 * @createTime: 2024/11/14 19:43
 * @contact: yuanxin9997@qq.com
 * @description: Redis 工具类
 */
public class RedisUtil {

    // private static final JedisPoolConfig config = new JedisPoolConfig();

    static {
        // Redis连接配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setMinIdle(2);
        config.setMaxWaitMillis(10 * 1000);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        JedisPool pool = new JedisPool(config, "hadoop162", 6379);

    }

    private static JedisPool pool;  // Redis 连接池

    public static Jedis getRedisClient() {
        Jedis jedis =  pool.getResource();
        jedis.select(1);  // 默认选择1号库
        return jedis;
    }

}
