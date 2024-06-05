package com.atguigu.chapter05_source;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/04 20:29
 * @contact: yuanxin9997@qq.com
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
//public class WaterSensor {
//    private String id;
//    private Long ts;
//    private Integer vc;
//
//    // 构造方法
//    public WaterSensor(String id, Long ts, Integer vc) {
//
//        this.id = id;
//
//        this.ts = ts;
//
//        this.vc = vc;
//    }
//    // getter和setter方法
//
//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public Long getTs() {
//        return ts;
//    }
//
//    public void setTs(Long ts) {
//        this.ts = ts;
//    }
//
//    public Integer getVc() {
//        return vc;
//    }
//
//    public void setVc(Integer vc) {
//        this.vc = vc;
//    }
//
//    // 重写toString方法
//
//
//    @Override
//    public String toString() {
//        return "WaterSensor{" +
//                "id='" + id + '\'' +
//                ", ts=" + ts +
//                ", vc=" + vc +
//                '}';
//    }
//}