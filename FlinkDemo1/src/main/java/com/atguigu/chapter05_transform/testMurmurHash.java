package com.atguigu.chapter05_transform;

import org.apache.flink.util.MathUtils;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/05 20:47
 * @contact: yuanxin9997@qq.com
 * @description: 测试MurmurHash
 */
public class testMurmurHash {
    public static void main(String[] Args) {
        int a= 0;
        int b=1;
        String c="偶数";
        String d="奇数";

        System.out.println(MathUtils.murmurHash(0) % 128);
        System.out.println(MathUtils.murmurHash(1) % 128);
        System.out.println(MathUtils.murmurHash(2) % 128);
        System.out.println(MathUtils.murmurHash(3) % 128);
        System.out.println(MathUtils.murmurHash(4) % 128);
        System.out.println(MathUtils.murmurHash(5) % 128);
        System.out.println(MathUtils.murmurHash(6) % 128);
        System.out.println(MathUtils.murmurHash(7) % 128);
        System.out.println(MathUtils.murmurHash(8) % 128);
//        System.out.println(c.hashCode());
//        System.out.println(d.hashCode());
        System.out.println(MathUtils.murmurHash(c.hashCode()) % 128);
        System.out.println(MathUtils.murmurHash(d.hashCode()) % 128);
    }
}
