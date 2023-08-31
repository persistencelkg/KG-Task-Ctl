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




}
