package org.kg.ctl.service;

import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.TaskDynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.time.LocalTime;

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
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getBatchSize();
    }

    /**
     * 任务执行时间
     *
     * @return 默认1秒
     */
    default long getSleepTime() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getSleepTime();
    }


    default boolean isBizPeek() {
        String bizPeek = getBizPeek();
        if (ObjectUtils.isEmpty(bizPeek)) {
            return false;
        }
        String[] split = bizPeek.split(JobConstants.LINE);
        Assert.isTrue(split.length == 2, "invalid job biz peek config:" + bizPeek);
        int start = Integer.parseInt(split[0]);
        int end = Integer.parseInt(split[1]);
        Assert.isTrue(start < end, "invalid job biz peek config:" + bizPeek);
        int hour = LocalTime.now().getHour();
        return start <= hour && hour <= end;
    }
}
