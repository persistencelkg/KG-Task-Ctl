package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/11/2 5:16 PM
 */
@Getter
@AllArgsConstructor
public enum TaskModeEnum {

    INC_SYNC("inc_sync", "增量同步"),

    FULL_SYNC("full_sync", "全量同步"),

    INC_CHECK("inc_check", "增量比对"),

    FUll_CHECK("full_check", "全量比对"),

    INC_ARCHIVE("inc_archive", "增量归档"),

    FULL_ARCHIVE("full_archive", "全量归档"),

//    RECOVER("recover", "数据恢复"),

    ;

    private final String mode;

    private final String description;


    private static final Map<String, TaskModeEnum> MAP = new HashMap<>();

    static {
        TaskModeEnum[] values = TaskModeEnum.values();
        for (TaskModeEnum value : values) {
            MAP.put(value.getMode(), value);
        }
    }

    public static TaskModeEnum getInstance(String mode) {
        return MAP.get(mode);
    }

    public static boolean isIncrementSync(String mode) {
        return Objects.nonNull(getInstance(mode)) && mode.startsWith("inc");
    }
}
