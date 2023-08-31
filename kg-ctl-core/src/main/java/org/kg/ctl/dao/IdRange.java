package org.kg.ctl.dao;

import lombok.Data;

/**
 * @description: 最大、最小id
 * @author: 李开广
 * @date: 2023/5/25 11:29 AM
 */
@Data
public class IdRange {
    private Long maxId;
    private Long minId;
}
