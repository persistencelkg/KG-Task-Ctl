package org.kg.ctl.service;

import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.TaskDynamicConfig;
import org.kg.ctl.dao.TaskGranularConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.time.LocalTime;

/**
 * 自适应高峰时段的频次控制服务
 */
public interface TaskGranularService {

    Logger log = LoggerFactory.getLogger(TaskGranularService.class);

    default TaskGranularConfig getConfig() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName());
    }

    /**
     * 定制化业务的高峰期
     *
     * @return
     */
    default String[] getBizPeek() {
        return getConfig().getBizPeekDuration();
    }

    /**
     * 任务执行时间
     *
     * @return 默认1秒
     */
    default long getSleepTime() {
        long sleepTime = TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getSleepTime();
        return isPeek() ? (long) (sleepTime * 1.25) : sleepTime;
    }

    /**
     * id连续查询时使用
     * @return
     */
    default int getDynamicDbQueryNumber() {
        TaskGranularConfig config = getConfig();
        int batchSize = config.getBatchSize();
        int maxBatchSize = config.getMaxBatchSize();
        return isPeek() ? batchSize : maxBatchSize;
    }

    default boolean isPeek() {
        String[] bizPeek = getBizPeek();
        if (ObjectUtils.isEmpty(bizPeek)) {
            return true;
        }
        int hour = LocalTime.now().getHour();
        for (String s : bizPeek) {
            if (isHitHighPeekRange(s, hour)) {
                return true;
            }
        }
        return false;
    }

    static boolean isHitHighPeekRange(String bizPeek, int hour) {
        String[] split = bizPeek.split(JobConstants.LINE);
        Assert.isTrue(split.length == 2, "invalid job biz peek config:" + bizPeek);
        int start = Integer.parseInt(split[0]);
        int end = Integer.parseInt(split[1]);
        Assert.isTrue(start < end, "invalid job biz peek config:" + bizPeek);
        return start <= hour && hour <= end;
    }

}
