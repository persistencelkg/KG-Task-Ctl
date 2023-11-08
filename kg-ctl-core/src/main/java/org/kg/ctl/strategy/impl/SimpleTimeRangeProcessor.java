package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.util.TaskUtil;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.util.Assert;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @description:
 * @author: 李开广
 * @date: 2023/8/23 2:14 PM
 */
public abstract class SimpleTimeRangeProcessor<T> extends AbstractTaskFromTo<T> {


    public SimpleTimeRangeProcessor(DbBatchQueryMapper<T> dbBatchQueryMapper) {
        super(dbBatchQueryMapper);
    }

    @Override
    protected void checkValid(TaskPo.InitialSnapShot initialSnapShot) {
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }

    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(initialSnapShot.getIndex(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        return initialSnapShot.getEndTime().isBefore(taskSegment.getEndTime().plusNanos(1));
    }


    public static void main(String[] args) {
        CronSequenceGenerator cronSequenceGenerator = new CronSequenceGenerator("0 0 12 * * ?");
        Date next = cronSequenceGenerator.next(new Date(System.currentTimeMillis()));
        LocalDateTime.ofInstant(Instant.ofEpochMilli(next.getTime()), ZoneOffset.ofHours(8));
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(next));
    }
}
