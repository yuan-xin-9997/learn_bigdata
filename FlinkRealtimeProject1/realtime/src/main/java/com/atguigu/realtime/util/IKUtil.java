package com.atguigu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/09/11 20:49
 * @contact: yuanxin9997@qq.com
 * @description: 分词工具类
 */
public class IKUtil {
    public static void main(String[] Args) throws IOException {
        System.out.println(split("我是中国人"));
        System.out.println(split("苹果手机 黑色手机 256G的手机"));
    }

    public static List<String> split(String keyword) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        // String -> Reader
        // StringReader 内存流
        StringReader reader = new StringReader(keyword);
        IKSegmenter seg = new IKSegmenter(reader, true);
        // IKSegmenter seg = new IKSegmenter(reader, false);
        Lexeme le = null;
        try {
            le = seg.next();
            while (le != null) {
                String text = le.getLexemeText();
                result.add(text);
                le = seg.next();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     *
     * @param keyword 要分词的输入
     * @param isDistinct true 表示对分词结果进行去重，false 表示不去重
     * @return
     * @throws IOException
     */
    public static List<String> split(String keyword, boolean isDistinct) throws IOException {
        if (isDistinct) {
            List<String> split = split(keyword);
            HashSet<String> set = new HashSet<>(split);
            return new ArrayList<>(set);
        }
        else
            return split(keyword);
    }
}






























