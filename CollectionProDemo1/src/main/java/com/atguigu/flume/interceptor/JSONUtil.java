package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JSONUtil {
    public static boolean isBoolValidate(String json){
        // 1.
        try {
            JSON.parseObject(json);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
}
