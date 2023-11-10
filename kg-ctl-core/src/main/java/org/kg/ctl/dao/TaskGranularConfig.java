package org.kg.ctl.dao;

import lombok.Data;
import org.kg.ctl.util.JsonUtil;

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

    /**
     * 业务的高峰时段
     */
    private String bizPeekDuration = "6->21";

    /**
     * 任务执行频次
     */
    private int batchSize = 500;

    /**
     * 每次执行后的休眠时间 毫秒
     */
    private long sleepTime = 1000L;

    /**
     * 业务场景
     */
    private String bizScene;

//    private Integer mixThreadCount;

    private boolean run;

    public static void main(String[] args) {
        TaskGranularConfig taskGranularConfig = new TaskGranularConfig();
//        taskGranularConfig.setMixedBiz(true);
        taskGranularConfig.setRun(false);
        taskGranularConfig.setBizScene("IO");
//        taskGranularConfig.setTimeSplitDimension("mintue");
        taskGranularConfig.setBizPeekDuration("6-21");
//        taskGranularConfig.setTimeSplitSize(1);
        System.out.println(JsonUtil.toJson(taskGranularConfig));
    }
}
