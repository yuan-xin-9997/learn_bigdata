package com.atguigu.realtime.function;

import com.atguigu.realtime.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/09/11 20:40
 * @contact: yuanxin9997@qq.com
 * @description:
 */
@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class IKAnalyzer extends TableFunction<Row> {
    public static void main(String[] Args) {

    }

    public void eval(String keyword) throws IOException {
        // keyword 用ik分词器进行分词 分出来几个词 就是几行 Row
        List<String> kws = IKUtil.split(keyword, true);
        for (String kw : kws) {
            collect(Row.of(kw));
        }
    }
}
