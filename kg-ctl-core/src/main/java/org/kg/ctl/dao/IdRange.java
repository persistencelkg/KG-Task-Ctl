package org.kg.ctl.dao;

import lombok.Data;

/**
 * Description: 最大、最小id
 * Author: 李开广
 * Date: 2023/5/25 11:29 AM
 */
@Data
public class IdRange {
    private Long maxId;
    private Long minId;
}
