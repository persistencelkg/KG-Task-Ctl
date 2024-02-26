package org.kg.ctl.strategy;

import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.service.DingDingService;
import org.springframework.beans.factory.InitializingBean;

import java.util.HashMap;
import java.util.Map;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/20 1:03 PM
 */
public interface TaskPostProcessor extends InitializingBean, DingDingService {

    Map<TaskModeEnum, TaskPostProcessor> TASK_MANAGER = new HashMap<>(4);

    void postProcessBeforeExecute(TaskSegment taskSegment);


    void postProcessAfterExecute(TaskSegment taskSegment, Integer resultSize);

    default TaskPostProcessor getPostProcessor(TaskModeEnum taskModeEnum) {
        return TASK_MANAGER.get(taskModeEnum);
    }
}
