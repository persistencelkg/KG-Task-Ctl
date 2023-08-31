package org.kg.ctl.service;

import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.TaskDynamicConfig;
import org.springframework.util.ObjectUtils;

import java.time.LocalTime;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 5:20 PM
 */
public interface TaskControlService extends TaskGranularService {

    /**
     * 本脚手架不提供具体实现，因为每个developer都有自己的梦想
     *
     * @return 自定义线程池
     */
    ExecutorService executorService();

    /**
     * 获取执行过程中同时执行的子任务数
     * 根据业务高峰期 & 根据自身服务特征决定，执行任务的线程数
     * 它的值取决于isMixedBiz() & getBizPeek()
     *
     * @return
     */
    default Integer getConcurrentThreadCount() {
        return getDefaultThreadCount();
    }

    default Integer getDefaultThreadCount() {
        return getDynamicConcurrentThreadNum(isMixedBiz());
    }


    /**
     * 每个业务开关都必须自己控制
     *
     * @return 开关结果
     */
    default boolean isRun() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).isRun();
    }

    /**
     * 获取动态运行的并发线程数
     *
     * @return
     */
    default int getDynamicConcurrentThreadNum(boolean isMixedBiz) {
        int count = Runtime.getRuntime().availableProcessors();
        if (!isMixedBiz) {
            // experience count
            return (int) Math.floor(count * 1.25);
        }
        String bizPeek = getBizPeek();
        int half = count >> 1;
        if (ObjectUtils.isEmpty(bizPeek)) {
            log.warn("job run with out biz peek");
            return half;
        }
        String[] split = bizPeek.split(JobConstants.LINE);
        try {
            int start = Integer.parseInt(split[0]);
            int end = Integer.parseInt(split[1]);
            if (start >= end) {
                return half;
            }
            int hour = LocalTime.now().getHour();
            if (start <= hour && hour <= end) {
                return half;
            }
            // experience count
            return (int) Math.ceil(count * 0.75);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return half;
    }
}
