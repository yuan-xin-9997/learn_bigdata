package com.atguigu.jedis.demos;

import redis.clients.jedis.Jedis;

import java.util.Set;

public class JedisDemo1 {
    public static void main(String[] args) {
        // 创建客户端对象
        Jedis jedis = new Jedis("hadoop102", 6379);
        jedis.auth("123456");

        // 使用
        String res = jedis.ping();
        System.out.println(res);

        // string, 如果string类型的key不存在，返回null，不是""
        jedis.set("java", "java");
        String r = jedis.get("java");
        // 获得了结果之后，需要做null值判断
        System.out.println(r);

        // set: 如果set类型对应的key不存在，返回[] 空集合，可以直接遍历的，不会报空指针异常，不是null
        jedis.sadd("aset", "11", "12", "13");
        Set<String> aset = jedis.smembers("aset");
        System.out.println(aset);

        jedis.shutdown();

        // 关闭客户端对象
        jedis.close();
    }
}
