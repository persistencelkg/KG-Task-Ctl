package org.kg.ctl.service;

import org.kg.ctl.dao.TaskDynamicConfig;
import org.kg.ctl.util.SpringUtil;

import java.util.concurrent.ExecutorService;

import static org.kg.ctl.config.CtlTaskThreadTaskConfiguration.IO_TASK;
import static org.kg.ctl.config.JobConstants.INTERNAL_PROCESSORS;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/23 5:20 PM
 */
public interface TaskControlService extends TaskGranularService {


    /**
     * 本脚手架不提供具体实现，因为每个developer都有自己的梦想
     *
     * @return 自定义线程池
     */
    default ExecutorService executorService() {
        return SpringUtil.getBean(IO_TASK, ExecutorService.class);
    }

    /**
     * 单次任务提交数：核数一半
     *
     * @return
     */
    default Integer getTaskSubmitCount() {
        return isPeek() ? TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getSubmitThreadCount() : INTERNAL_PROCESSORS >> 1;
    }

    /**
     * 每个业务开关都必须自己控制
     *
     * @return 开关结果
     */
    default boolean isRun() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).isRun();
    }


}
