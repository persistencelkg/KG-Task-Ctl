package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/11/2 5:16 PM
 */
@Getter
@AllArgsConstructor
public enum TaskModeEnum {
    INCR_SYNC(0, "增量同步"),
    ALL_INF_SYNC(1, "全量同步")
    ;

    private final Integer mode;

    private final String description;


}
