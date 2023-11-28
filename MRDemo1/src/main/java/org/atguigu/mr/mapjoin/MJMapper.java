package org.atguigu.mr.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> map = new HashMap<>(); // 用来缓存pd.txt中的内容
    private Text outKey = new Text(); //  创建Key对象

    /**
     * 缓存pd.txt中的数据（不能用带参数构造器，因为框架调用的是空参构造器）
     * setup方法在MapTask方法执行前被MR框架调用一次
     * 作用：初始化
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        FileSystem fs = null;
        FSDataInputStream fis = null;
        try {
            // 创建客户端对象（为了创建流）
            fs = FileSystem.get(context.getConfiguration());
            // 创建流
            // 2.1 换取缓存文件的路径
            URI[] cacheFiles = context.getCacheFiles();
            URI uri = cacheFiles[0];
            // 2.2  创建流
            fis = fs.open(new Path(uri));  // 字节流
            // 3 读取数据（一行一行读取数据）
            // (1) 字符缓冲流里面套字符流，但是我们拿到的是字节流，需要将字节流转换成字符流
            BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line = "";
            while((line = br.readLine()) != null ){
                // 4.将数据切割
                String[] split = line.split("\t");
                // 5. 将内容缓存到map中
                map.put(split[0], split[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }finally {
             // 6. 关闭资源（不合适在cleanup里面关，因为map不需要用了）
            if (fs != null){
                fs.close();
            }
            if (fis != null){
                fis.close();
            }
        }
    }

    /**
     * 关闭资源
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    /**
     * 读取order.txt中的内容，并进行Join操作
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1. 切割数据：1001 01  1
        String[] split = value.toString().split("\t");
        // 2. 字符串拼接：id pname amount
        String info = split[0] + " " + map.get(split[1]) + " " + split[2];
        // 3. 封装key, value
        // 给Key赋值
        outKey.set(info);
        // 4. 将key,value写入到环形缓冲区
        context.write(outKey, NullWritable.get());
    }
}
