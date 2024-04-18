package com.atguigu.jedis.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

public class JedisPoolDemo1 {
    public static void main(String[] args) {
        // 为连接池做一些配置

        //主要配置
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10); //最大可用连接数
        jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
        jedisPoolConfig.setMinIdle(5); //最小闲置连接数
        jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
        jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
//        jedisPoolConfig.setTestOnBorrow(true); // 取连接的时候，先对连接进行测试
        /*
        * redis踩坑记录:setTestOnBorrow导致Could not get a resource from the pool,Unable to validate object
        *
        * */

        // 创建Jedis连接池
        JedisPool jedisPool = new JedisPool(jedisPoolConfig,"hadoop102", 6379);
        // 从连接池中借一个客户端连接对象
            Jedis jedis = jedisPool.getResource();
            jedis.auth("123456");

        try {

            // 使用
            String res = jedis.ping();
            System.out.println(res);
        }finally {
            // 关闭客户端，把借的连接池返回池子
            jedis.close();
            jedisPool.close();
        }



    }
}
