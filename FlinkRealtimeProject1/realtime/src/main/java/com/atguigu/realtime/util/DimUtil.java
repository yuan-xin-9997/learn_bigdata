package com.atguigu.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.Constant;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/31 19:57
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class DimUtil {
    public static void main(String[] Args) {

    }

    /**
     * 从Phoenix(HBase)中读取维度数据
     * @param phoenixConn
     * @param table
     * @param id
     * @return
     */
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String table, String id) {
        String sql = "select * from " + table + " where id = ?";
        String[] args = {id};
        // 一个通用的JDBC查询方法，返回值可能有多行，所以返回list集合
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);
        return list.get(0);  // 对当前SQL语句，一定只有一行，直接get(0)
    }

    /**
     * 从Redis中读取维度数据 + 如果未读取到则从Phoenix中读取
     * @param redisClient
     * @param phoenixConn
     * @param table
     * @param id
     * @return
     */
    public static JSONObject readDim(Jedis redisClient, DruidPooledConnection phoenixConn, String table, String id) {
        // 1. 从Redis中读取维度数据
        JSONObject dim = readDimFromRedis(redisClient, table, id);
        // 2. 如果存在，则直接返回
        if (dim == null) {
            System.out.println("从Phonex中读取维度数据" + System.currentTimeMillis() + " - " + table + " - " + id);
            // 3. 如果不存在，则从Phoenix中读取，返回，把维度写入到redis中
            dim = readDimFromPhoenix(phoenixConn, table, id);
            // 维度写入到Redis中
            writeDimToRedis(redisClient, table, id, dim);
        }else{
            System.out.println("从Redis中读取维度数据" + System.currentTimeMillis() + " - " + table + " - " + id);
        }
        return dim;
    }

    /**
     * 将维度数据写入到Redis中
     * @param redisClient
     * @param table
     * @param id
     * @param dim
     */
    private static void writeDimToRedis(Jedis redisClient, String table, String id, JSONObject dim) {
        String key = table + ":" + id;
        String value = dim.toJSONString();
        // redisClient.set(key, value);
        // redisClient.expire(key, 60 * 60 * 24);  // 设置过期时间，24小时
        redisClient.setex(key, Constant.TWO_DAY_SECONDS, value);  // 设置过期时间，24小时
    }

    /**
     * 从Redis中读取维度数据
     * @param redisClient
     * @param table
     * @param id
     * @return
     */
    private static JSONObject readDimFromRedis(Jedis redisClient, String table, String id) {
        String key = table + ":" + id;
        String value = redisClient.get(key);
        if (value != null) {
            return JSONObject.parseObject(value);
        }
        return null;
    }
}

/**
 * Redis中数据类型的选择 (最终选择string)
 *
 * string  √
 *      key     表名 + id
 *      value   {json格式的字符串}
 *      好处：读写方便
 *      坏处：一个id一条数据，key的数量会很多，后期容易产生冲突
 *      解决方案：专门放入一个库中
 *      会给key添加ttl，过期时间，冷数据会自动删除，每个key可以单独设置
 * list
 *      key    表名
 *      value  [{json格式字符串}, {json格式字符串}, {json格式字符串}, ...]
 *      好处：可以只占用6个key
 *      坏处：写数据方便，读数据的时候需要遍历list
 *      ×
 * set
 *      相比于list，set多了去重功能，不适合
 * hash
 *      key    field     value
 *      表名   id        {json格式字符串}
 *      好处：key数量少
 *           相比list，读写方便
 *      坏处：给key设置ttl，影响到整个表，没有办法单独对每个key设置ttl
 */


























