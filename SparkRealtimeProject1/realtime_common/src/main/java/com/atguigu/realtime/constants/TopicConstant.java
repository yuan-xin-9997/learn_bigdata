package com.atguigu.realtime.constants;

/**
 * 常量（static final修饰) 一般声明在接口中
 *      无需实例化对象，通过类名.变量可以获取常量
 *
 *      因此常量类应该是太监类：
 *          1.抽象类
 *          2.接口类
 */
public interface TopicConstant {

    String ORIGINAL_LOG = "base_log";

    String STARTUP_LOG = "REALTIME_STARTUP_LOG";
    String ACTION_LOG = "REALTIME_ACTION_LOG";

    String ORDER_INFO = "REALTIME_DB_ORDER_INFO";
    String ORDER_DETAIL = "REALTIME_DB_ORDER_DETAIL";
}
