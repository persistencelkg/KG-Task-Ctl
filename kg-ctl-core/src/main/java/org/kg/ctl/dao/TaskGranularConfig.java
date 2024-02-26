package org.kg.ctl.dao;

import lombok.Data;

/**
 * TODO n多个job 如何隔离控制，是否应该基于表呢？
 * 基于任务的量级、频次控制
 *
 * Description: 任务的粒度控制
 * Author: 李开广
 * Date: 2023/5/23 2:31 PM
 */
@Data
public class TaskGranularConfig {

    private String bizPeekDuration = "6->21";
    /**
     * 低峰最大查询数据量
     */
    private int maxBatchSize = 500;

    /**
     * 高峰最大查询数据量
     */
    private int batchSize = 300;

    /**
     * 每次执行后的休眠时间 毫秒
     */
    private long sleepTime = 1000L;

    private int submitThreadCount = 2;

    /**
     * 业务场景
     */
    private String bizScene;

    private String url;
    private String secret;

    /**
     * 控制开关
     */
    private boolean run;
}
