package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Author 李开广
 * Date 2023/4/13 6:33 下午
 */
@AllArgsConstructor
@Getter
public enum TaskStatusEnum {
    /**
     * 执行中
     */
    WORKING(1, "working"),
    /**
     * 其他机器执行中
     */
    OCCUPY(2, "occupy"),
    /**
     * 已完成
     */
    FINISHED(3, "finished")
        ;
    private final Integer code;

    private final String description;


    private static final Map<Integer, TaskStatusEnum> MAP = new HashMap<>();

    static {
        TaskStatusEnum[] values = TaskStatusEnum.values();
        for (TaskStatusEnum value : values) {
            MAP.put(value.getCode(), value);
        }
    }

    public static TaskStatusEnum getInstance(Integer code) {
        return MAP.get(code);
    }
}
