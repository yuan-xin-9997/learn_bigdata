package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/27 19:34
 * @contact: yuanxin9997@qq.com
 * @description: Flink流处理 处理 无界流数据数据（Socket端口模拟数据）  WordCount  Lambda表达式
 *
 * Lambda表达式
 * 什么是函数？进入一个x 出来一个y 通过某个计算规则f(x)
 * 函数式接口
 *      这个接口只有1个方法需要实现
 *      @FunctionalInterface 注解 表示这个接口是函数式接口
 * 在Java1.8中，函数式接口可以改写为Lambda表达式
 * (参数列表) -> {方法的具体实现逻辑}       -- 类似Scala
 * 当参数列表只有一个参数时，可以省略小括号
 * 当方法体中只有一条语句时，可以省略大括号。当唯一的这一行是返回值，return也可以省略
 */
public class Flink01_Streaming_unbounded_WordCount_lambda {
    public static void main(String[] Args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 2. 读取数据 切分 转换
        DataStreamSource<String> socketedTextStream = executionEnvironment.socketTextStream("hadoop102", 9999);
        socketedTextStream
                // 在改写为Lambda表达式的过程中，由于内部出现了泛型信息擦除，会导致编译器无法判断该Lambda表达式的返回值类型，所以需要显式的指定
                .flatMap((FlatMapFunction<String, String>) (line, out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        })
                .returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Long>>) word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
        // 3. 分组 sum
                .keyBy((KeySelector<Tuple2<String, Long>, String>) tuple2 -> tuple2.f0)
                .sum(1)
                .print();

        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

        // 4. 执行
        executionEnvironment.execute();
    }
}
