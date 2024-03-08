package org.kg.ctl.service;

/**
 * Description: 表的元数据
 * Author: 李开广
 * Date: 2024/2/21 9:07 PM
 */
public interface TableMetaData {

    /* this is only an acknowledgment judge，id whether increment depends on your biz scene
     * 以下场景不要set
     * 1. 物理归档过的表，因为扫描数据会断层
     * 2. 非自增整型的id
     */
    default boolean idIncrement() {
        return false;
    }

    String uniqueKey();

    /**
     * 决定内置mp的update 、insert时的sql语句，是否需要考虑使用源表id到目的表的set操作，基于MP的方式完成
     * 默认是不要id
     * 以下场景前往不要使用id
     * 1.无论目的表还是源表 是分表
     * 2.表中有其他字段作为唯一索引
     *
     * @see com.baomidou.mybatisplus.extension.injector.methods.AlwaysUpdateSomeColumnById
     * @see com.baomidou.mybatisplus.extension.injector.methods.InsertBatchSomeColumn
     * 唯一索引不是id，那么过滤id的insert和update
     * 仅仅适用在单表情况
     */
    default boolean updateInsertSetId() {
        return false;
    }
}
