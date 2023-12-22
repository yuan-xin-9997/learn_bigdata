package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 判断单个事件是否符合json格式
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 1. 获取body
        String message = new String(event.getBody(), StandardCharsets.UTF_8);
        // 2. 判断是否符合json
        if(JSONUtil.isBoolValidate(message)){
            // 3. 符合JSON格式，在头部插入type:true
            event.getHeaders().put("type", "true");
        }else{
            // 4.不符合JSON格式，在头部插入type:false
            event.getHeaders().put("type", "false");
        }
//        System.out.println("body=     " + event.getBody());
//        System.out.println("header=     " + event.getHeaders());
        System.out.println("evnet=      " + event.toString());
        String result = event.getHeaders().entrySet()
                .stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .reduce((a, b) -> a + "," + b)
                .orElse("");
//        System.out.println("header=   " + result );
        System.out.println(message);
        return null;
    }

    /**
     * 遍历批量事件中的每个事件，根据是否符合json格式，移除不符合json格式的
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        // 1.获取迭代对象
        Iterator<Event> iterator = events.iterator();
        // 2.遍历
        while(iterator.hasNext()){
            // 3.获取事件对象
            Event next = iterator.next();
            // 4.调用单个拦截器
            intercept(next);
            // 5. 移除不符合json格式的
            String flag = next.getHeaders().get("type");
            if(flag.equals("false")){
                // 6. 移除掉不符合json格式的事件
                iterator.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
