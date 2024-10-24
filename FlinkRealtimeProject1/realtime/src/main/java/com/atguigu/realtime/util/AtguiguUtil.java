package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/14 21:21
 * @contact: yuanxin9997@qq.com
 * @description: 工具类
 */
public class AtguiguUtil {
    public static void main(String[] Args) {

    }

    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }

    public static <T>List<T> toList(Iterable<T> it) {
        ArrayList<T> list = new ArrayList<>();
        // it.forEach(e -> list.add(e));
        it.forEach(list::add);
        return list;
    }

    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }

    public static Long toTimeStamp(String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();
    }
}
