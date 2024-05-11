package com.atguigu.realtime.utils;

import redis.clients.jedis.Jedis;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 12:11
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class RedisUtil {

    public static Jedis getJedis(){
        String host = PropertiesUtil.getProperty("redis.host");
        String port = PropertiesUtil.getProperty("redis.port");
        return new Jedis(host, Integer.parseInt(port));
    }

    public static void main(String[] Args) {

    }
}
