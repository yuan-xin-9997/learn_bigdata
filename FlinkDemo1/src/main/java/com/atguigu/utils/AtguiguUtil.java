package com.atguigu.utils;

import com.atguigu.chapter05_source.WaterSensor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:25
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class AtguiguUtil {
    public static void main(String[] Args) {

    }

    public static <T>List<T> toList(Iterable<T> elements) {
        ArrayList<T> list = new ArrayList<>();
        // elements.forEach(list::add);
        for (T t : elements) {
            list.add(t);
        }
        return list;
    }

    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
}
