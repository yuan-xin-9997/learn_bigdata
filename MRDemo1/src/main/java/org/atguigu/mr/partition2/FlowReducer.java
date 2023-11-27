package org.atguigu.mr.partition2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
* Reducer阶段会运行MapTask，MapTask会调用Reducer类型
* 作用：在该类中实现业务逻辑代码，Reduce阶段的
*
* Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
*   第一组泛型：
*       KEYIN：读取的key的类型（Mapper写出的key的类型）
*       VALUEIN：读取的value的类型（Mapper写出的value的类型）
*   第二组泛型：
*       KEYOUT：写出的key的类型（在这里是手机号的类型）
*       VALUEOUT：写出的value的类型（在这里是FLowBean）
* */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outValue = new FlowBean();

    /**
     * 1.在reduce方法中实现需要在ReduceTask中实现的业务逻辑代码
     * 2. reduce方法在被循环调用，每调一次传入一组数据，（在这key值相同为一组）
     *
     *
     *
     * @param key 读取的key
     * @param values 读取的所有的value
     * @param context 上下文（在这用来将key，value写数据）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // super.reduce(key, values, context);
        long sumUpFlow = 0; // 总上行
        long sumDownFlow = 0; // 总下行
        // 1. 遍历所有的values
        for (FlowBean value : values) {
            // 将上行流量累加
            sumUpFlow += value.getUpFlow();
            // 将下行流量累加
            sumDownFlow += value.getDownFlow();

        }
        // 2. 封装key，value，封装FLowBean
        // 给value赋值
        outValue.setUpFlow(sumUpFlow);
        outValue.setDownFlow(sumDownFlow);
        outValue.setSumFlow(outValue.getUpFlow() + outValue.getDownFlow());

        // 3. 将key,value写数据
        context.write(key, outValue);
    }
}
