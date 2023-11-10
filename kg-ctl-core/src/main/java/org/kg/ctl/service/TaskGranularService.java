package org.kg.ctl.service;

import org.kg.ctl.dao.TaskDynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TaskGranularService {

    Logger log = LoggerFactory.getLogger(TaskGranularService.class);

    /**
     * 定制化业务的高峰期
     *
     * @return
     */
    default String getBizPeek() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getBizPeekDuration();
    }

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
