package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.util.Assert;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/23 2:14 PM
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
    protected boolean dynamicExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        int n = getConcurrentThreadCount();
        ExecutorService executorService = executorService();
        CountDownLatch countDownLatch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            TaskSegment taskSegment = workingTaskSegment.get(i);
            try {
                executorService.execute(() -> {
                    executeTask(taskSegment, buildExecuteFunction(initialSnapShot));
                    countDownLatch.countDown();
                });
            } catch (Exception e) {
                e.printStackTrace();
                dingErrorLog(MessageFormat.format("快照：{0} 执行出现异常：{1}", taskSegment, e));
            }
            if (!isRun()) {
                dingErrorLog(MessageFormat.format("快照:{0}已经停止，当前执行快照：{1}", taskSegment.getTaskId(), taskSegment));
                return false;
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        return true;

    }

    @Override
    protected Function<TaskSegment, Boolean> buildExecuteFunction(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(initialSnapShot.getIndex(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }


    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        return initialSnapShot.getEndTime().isBefore(taskSegment.getEndTime().plusNanos(1));
    }

    @Override
    protected void processTaskByScene(TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot) {
        splitAndRunTask(taskJob.getId(), taskSnapShot, null);
    }


    public static void main(String[] args) {
        CronSequenceGenerator cronSequenceGenerator = new CronSequenceGenerator("0 0 12 * * ?");
        Date next = cronSequenceGenerator.next(new Date(System.currentTimeMillis()));
        LocalDateTime.ofInstant(Instant.ofEpochMilli(next.getTime()), ZoneOffset.ofHours(8));
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(next));
    }
}
