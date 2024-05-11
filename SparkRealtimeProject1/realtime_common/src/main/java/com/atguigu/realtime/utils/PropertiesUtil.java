package com.atguigu.realtime.utils;

import java.util.Properties;
import java.util.ResourceBundle;

public class PropertiesUtil {
    //读取配置文件，将读到的信息封装为1个ResourceBundle
    // ResourceBundle.getBundle()  读取类路径 resource 下，以xx.properties命名的文件，传入xx
    private static ResourceBundle props = ResourceBundle.getBundle("config");

    // 获取指定属性的方法
    public static String getProperty(String name) {
        return props.getString(name);
    }

    public static void main(String[] args) {
        System.out.println(getProperty("es.nodes"));
    }
}
