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