package org.kg.ctl.service;

import org.kg.ctl.dao.TaskDynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TaskGranularService {

    Logger log = LoggerFactory.getLogger(TaskGranularService.class);

    /**
     * 当前是否是混合业务，改值对线程任务数有直接影响
     * 默认是业务和job混合
     *
     * @return 是混合业务
     */
//    default boolean isMixedBiz() {
//        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).isMixedBiz();
//    }

    /**
     * 定制化业务的高峰期
     *
     * @return
     */
    default String getBizPeek() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getBizPeekDuration();
    }


//    default TemporalAmount getTaskSplitDuration() {
//        return TaskTimeSplitEnum.getDuration(getTaskSplitTimeStr(), getTaskSplitTimeSize());
//    }

    /**
     * 任务切分的时间粒度
     *
     * @return 默认按 1 minute 切分
     */
//    default Integer getTaskSplitTimeSize() {
//        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getTimeSplitSize();
//    }

//    default String getTaskSplitTimeStr() {
//        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getTimeSplitDimension();
//    }

    /**
     * 批量任务大小
     *
     * @return 默认500
     */
    default Integer getBatchSize() {
        return  TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getBatchSize();
    }

    /**
     * 任务执行时间
     *
     * @return 默认1秒
     */
    default long getSleepTime() {
        return  TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getSleepTime();
    }


}
