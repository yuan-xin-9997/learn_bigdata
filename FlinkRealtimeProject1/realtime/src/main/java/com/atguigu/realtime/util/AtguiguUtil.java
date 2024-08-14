package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/14 21:21
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class AtguiguUtil {
    public static void main(String[] Args) {

    }

    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
}
