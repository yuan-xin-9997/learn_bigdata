package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class CustomInterceptor implements Interceptor {
    /**
     * 初始化方法
     */
    @Override
    public void initialize() {

    }

    /**
     * 拦截单个事件的方法
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 1.获取数据
        byte[] body = event.getBody();
        // 2. 判断
        if(body[0] >= 'a' && body[0] <= 'z' || body[0] >= 'A' && body[0] <= 'Z'){
            event.getHeaders().put("type", "letter");
        }
        if(body[0] >= '0' && body[0] <= '9'){
            event.getHeaders().put("type", "number");
        }
        // 其他情况，此处略过
        return event;
    }

    /**
     * 拦截批量事件的方法
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
//        ArrayList<Event> eventArrayList = new ArrayList<>();
//        // 1. 遍历
//        for (Event event : events) {
//            Event event1 = intercept(event);
//            eventArrayList.add(event1);
//        }
//        return eventArrayList;

        // 1. 遍历
        for (Event event : events) {
            intercept(event);  // 地址传递
        }
        return events;
    }

    /**
     *
     */
    @Override
    public void close() {

    }

    // 创建静态内部类
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
