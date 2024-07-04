package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/04 20:57
 * @contact: yuanxin9997@qq.com
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotItem {
        private Long itemId;
        private Long windowEnd;
        private Long count;
}
