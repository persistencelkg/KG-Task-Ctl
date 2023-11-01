package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskTimeSplitEnum;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 2:14 PM
 */
public abstract class SimpleTimeRangeProcessor<T> extends AbstractTaskFromTo<T> {

    @Override
    protected void checkValid(TaskPo.InitialSnapShot initialSnapShot) {
        Assert.isTrue(Objects.nonNull(initialSnapShot.getStartTime()) && Objects.nonNull(initialSnapShot.getEndTime()), "your choose table id with time range， but not support valid range");
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }

    @Override
    protected List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot) {
        return TaskUtil.list(taskId, initialSnapShot.getStartTime(), initialSnapShot.getEndTime(),
                TaskTimeSplitEnum.getDuration(initialSnapShot.getSyncDimension(), initialSnapShot.getSyncInterval()));
    }

    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(initialSnapShot.getIndex(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegement) {
        return initialSnapShot.getEndTime().isBefore(taskSegement.getEndTime());
    }

}
