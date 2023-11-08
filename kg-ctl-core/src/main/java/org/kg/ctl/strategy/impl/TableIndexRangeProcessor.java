package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.Assert;

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 3:19 PM
 */
public abstract class TableIndexRangeProcessor<T> extends AbstractTaskFromTo<T> {



    public TableIndexRangeProcessor(DbBatchQueryMapper<T> dbBatchQueryMapper) {
        super(dbBatchQueryMapper);
    }

    @Override
    protected void checkValid(TaskPo.InitialSnapShot taskSnapShot) {
        Assert.isTrue(Objects.nonNull(taskSnapShot.getTableStart()) && Objects.nonNull(taskSnapShot.getTableEnd()), "your choose table id with time range， but not support valid id range");
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }

    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(task.getSnapshotValue(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }

    //    @Override
//    protected List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot, Integer Id) {
//        TemporalAmount duration = TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval());
//        List<TaskUtil.TimeSegment> timeRangeList = TaskUtil.list(initialSnapShot.getStartTime(), initialSnapShot.getEndTime(), duration);
//        List<TaskSegment> list = new ArrayList<>();
//        for (TaskUtil.TimeSegment val : timeRangeList) {
//            List<TaskSegment> tempList = TaskUtil.list(taskId, initialSnapShot.getTableStart(), initialSnapShot.getTableEnd(), this.getBatchSize());
//            tempList.forEach(temp -> {
//                temp.setStartTime(val.getStart());
//                temp.setEndTime(val.getEnd());
//            });
//            list.addAll(tempList);
//        }
//        return list;
//    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        return initialSnapShot.getEndTime().isBefore(taskSegment.getEndTime().plusNanos(1)) && initialSnapShot.getTableEnd() <= (taskSegment.getEndIndex());
    }
}
