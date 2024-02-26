package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/1/20 4:15 PM
 */
@AllArgsConstructor
@Getter
public enum InsertModeEnum {

    INSERT_IGNORE("insert_ignore"),
    COVER("cover_write")
    ;

    private final String mode;
}
