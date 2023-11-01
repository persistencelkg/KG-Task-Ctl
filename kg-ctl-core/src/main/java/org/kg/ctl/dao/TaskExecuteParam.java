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
     * 同步纬度，当选择时间纬度才会使用
     * such as： year, month，day
     */
    private String syncDimension;
    /**
     * 同步间隔
     */
    private Integer syncInterval;





}
