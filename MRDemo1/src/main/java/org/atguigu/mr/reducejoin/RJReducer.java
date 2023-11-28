package org.atguigu.mr.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RJReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     *          输入数据
     *          key                                 value
     *          0  1  0 小米                         null
     *          1001 1 1 “”                         null
     *          1004 1 4 “”                         null
     *
     * 1. 获取第一组数据的key的pnmae值
     * 2. 遍历后面所得数据（key,value）
     * 3. 将第一组数据的key和pname替换后面所有key的pnmae
     * 4. 写key,value
     * 注意：当遍历value的时候，key就会指向value对应的key
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取迭代器
        Iterator<NullWritable> iterator = values.iterator();
        // 指针下移（获取第一条数据）
        NullWritable next = iterator.next();
        // 1.获取第一条数据的pname
        String pname = key.getPname();

        // 2.遍历后面所得数据（key,value）
        while (iterator.hasNext()){
            iterator.next(); //指针下移
            // 3. 将第一组数据的key和pname替换后面所有的key和pnmae
            key.setPname(pname);
            // 4. 写key,value
            context.write(key, NullWritable.get());
        }

    }
}
