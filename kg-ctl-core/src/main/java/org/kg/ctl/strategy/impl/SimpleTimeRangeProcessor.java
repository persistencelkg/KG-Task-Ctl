package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.util.TaskUtil;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 2:14 PM
 */
public abstract class SimpleTimeRangeProcessor<T> extends AbstractTaskFromTo<T> {

    @Override
    protected void checkValid(TaskPo.InitialSnapShot initialSnapShot) {
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }

    @Override
    protected List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot) {
        TemporalAmount duration = TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval());
        return TaskUtil.list(taskId, initialSnapShot.getStartTime(), initialSnapShot.getEndTime(), duration);
    }

    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(initialSnapShot.getIndex(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegement) {
        return initialSnapShot.getEndTime().isBefore(taskSegement.getEndTime().plusNanos(1));
    }


    public static void main(String[] args) {
        CronSequenceGenerator cronSequenceGenerator = new CronSequenceGenerator("0 0 12 * * ?");
        Date next = cronSequenceGenerator.next(new Date(System.currentTimeMillis()));
        LocalDateTime.ofInstant(Instant.ofEpochMilli(next.getTime()), ZoneOffset.ofHours(8));
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(next));
    }
}
