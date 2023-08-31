package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author likaiguang
 * @date 2023/4/13 6:33 下午
 */
@AllArgsConstructor
@Getter
public enum TaskStatusEnum {
    /**
     * 初始化状态
     */
    DEFAULT(0),
    /**
     * 执行中
     */
    WORKING(1),
    /**
     * 其他机器执行中
     */
    OCCUPY(2),
    /**
     * 已完成
     */
    FINISHED(3)
        ;
    private final Integer code;
}
