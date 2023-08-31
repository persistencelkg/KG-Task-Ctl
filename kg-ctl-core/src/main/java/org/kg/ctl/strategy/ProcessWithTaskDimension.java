package org.kg.ctl.strategy;

import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskDimensionEnum;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface ProcessWithTaskDimension {

    /**
     * 任务是否完成
     * @param lastSegment 最后一个分片
     * @param initialSnapShot 全局快照data
     * @return 成功与否
     */
    boolean isFinish(TaskSegment lastSegment, TaskPo.InitialSnapShot initialSnapShot);

    /**
     * 前置校验
     * @param initialSnapShot 全局快照
     * @return 是否校验成功
     */
    void doPrepareCheck(TaskDimensionEnum initialSnapShot);

    /**
     * 根据全局任务划分快照
     * @param taskId 主任务id，全局唯一
     * @param initialSnapShot 全局快照data
     * @return 可以执行的任务快照
     */
    List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot);

    /**
     * 具体执行业务逻辑
     * @param taskId： 主任务id
     * @param initialSnapShot: 全局任务
     */
    void doTask(String taskId, TaskSegment initialSnapShot);

}
