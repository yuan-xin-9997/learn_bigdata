package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/1/7 17:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;
}