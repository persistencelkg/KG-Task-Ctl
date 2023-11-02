package org.kg.ctl.dao;

import lombok.Data;
import org.springframework.util.StringUtils;

import java.util.Collection;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 9:20 PM
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
     */

    private Integer mode;

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
