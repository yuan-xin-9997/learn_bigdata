package com.atguigu.chapter05_source;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/03 20:49
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class WaterSensor {

    private String ws001;

    private long ts;
    private int vc;

    public WaterSensor(String ws001, long l, int i) {
        ws001=ws001;
        ts=l;
        vc=i;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "ws001='" + ws001 + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    public static void main(String[] Args) {

    }
}
