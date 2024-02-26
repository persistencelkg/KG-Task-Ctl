package org.kg.ctl.dao;

import lombok.Data;
import org.kg.ctl.dao.enums.InsertModeEnum;

import java.util.Collection;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/23 9:20 PM
 */
@Data
public class TaskExecuteParam {

    private String targetTime;


    private String startTime;


    private String endTime;


    // 当前仅支持table_X 格式的分表

    private String tablePreFix;


    private String tableRange;


    private String targetBizId;


    private Collection<?> dataList;

    /**
     * 同步模式
     * @see org.kg.ctl.dao.enums.TaskModeEnum
     */

    private String mode;

    /**
     * 插入模式，默认写覆盖
     * @see InsertModeEnum
     */
    private String insertMode = InsertModeEnum.COVER.getMode();

    /**
     * 同步间隔: P1D
     */

    private String syncInterval;

    /**
     * 同步周期：T-1D T+1D T-1H TOD
     */
    private String syncPeriod;


    /**
     * 在当前时间多久之前开始同步
     */
    private String beforeNowInterval;

}
