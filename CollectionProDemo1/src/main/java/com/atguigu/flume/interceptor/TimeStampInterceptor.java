package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 拦截单个事件
     * 将数据中的时间戳取出来，放入头部
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 1. 获取数据字符串
        String bodyStr = new String(event.getBody(), StandardCharsets.UTF_8);
        // 2. 解析为JSON对象
        JSONObject jsonObject = JSON.parseObject(bodyStr);
        // 3. 获取时间戳（毫秒级别的）
        Long ts = jsonObject.getLong("ts");
        // 4. 将时间戳放入头部
        event.getHeaders().put("timestamp", ts.toString());

        System.out.println(bodyStr);
        System.out.println(event.getHeaders().toString());
        return event;
    }

    /**
     * 拦截多个事件
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
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
