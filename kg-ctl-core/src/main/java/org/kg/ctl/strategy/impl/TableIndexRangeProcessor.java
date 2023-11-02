package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 3:19 PM
 */
public abstract class TableIndexRangeProcessor<T> extends AbstractTaskFromTo<T> {


    @Override
    protected void checkValid(TaskPo.InitialSnapShot taskSnapShot) {
        Assert.isTrue(Objects.nonNull(taskSnapShot.getMinId()) && Objects.nonNull(taskSnapShot.getMaxId()), "your choose table id with time range， but not support valid id range");
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }


    @Override
    protected List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot) {
        TemporalAmount duration = TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval());
        List<TaskUtil.TimeSegment> timeRangeList = TaskUtil.list(initialSnapShot.getStartTime(), initialSnapShot.getEndTime(), duration);
        List<TaskSegment> list = new ArrayList<>();
        for (TaskUtil.TimeSegment val : timeRangeList) {
            List<TaskSegment> tempList = TaskUtil.list(taskId, initialSnapShot.getMinId(), initialSnapShot.getMaxId(), this.getBatchSize());
            tempList.forEach(temp -> {
                temp.setStartTime(val.getStart());
                temp.setEndTime(val.getEnd());
            });
            list.addAll(tempList);
        }
        return list;
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegement) {
        return initialSnapShot.getEndTime().isBefore(taskSegement.getEndTime().plusNanos(1)) && initialSnapShot.getMaxId() <= (taskSegement.getEndIndex());
    }
}
