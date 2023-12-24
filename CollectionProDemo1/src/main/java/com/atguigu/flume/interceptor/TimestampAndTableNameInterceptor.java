package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 拦截单个事件
     *      将数据中的表名和时间戳放到Header中
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 1.获取body，转为字符串
        String bodyStr = new String(event.getBody(), StandardCharsets.UTF_8);
        // 2. 获取JSON对象
        JSONObject jsonObject = JSONObject.parseObject(bodyStr);
        // 3. 获取表名和时间戳
        String tableName = jsonObject.getString("table");
        long ts = jsonObject.getLong("ts") * 1000;
        // 4. 放置到Header
        event.getHeaders().put("table", tableName);
        event.getHeaders().put("timestamp", String.valueOf(ts));
        return event;
    }

    /**
     * 拦截批量事件
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
