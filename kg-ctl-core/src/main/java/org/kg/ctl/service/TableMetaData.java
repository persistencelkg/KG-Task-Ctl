package org.kg.ctl.service;

/**
 * Description: 表的元数据
 * Author: 李开广
 * Date: 2024/2/21 9:07 PM
 */
public interface TableMetaData {

    /* this is only an acknowledgment judge，id whether increment depends on your biz scene
     */
    boolean idIncrement();

    String uniqueKey();
}
