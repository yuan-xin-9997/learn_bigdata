package org.atguigu.mr.partition2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
* 自定义分区
*
* Partitioner<KEY, VALUE>
*    Key : map 输出的key
*    value： map输出的value
* */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    /**
     * 返回分区号
     * @param text the key to be partioned. mapper输出的key
     * @param flowBean the entry value. mapper输出的value
     * @param numPartitions the total number of partitions.  reducetask的数量
     * @return
     *
     * 期望：手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNumber = text.toString();
        if(phoneNumber.startsWith("136")){
            return 0;
        } else if (phoneNumber.startsWith("137")) {
            return 1;
        } else if (phoneNumber.startsWith("138")) {
            return 2;
        } else if (phoneNumber.startsWith("139")) {
            return 3;
        }else {
            return 4;
        }
    }
}
