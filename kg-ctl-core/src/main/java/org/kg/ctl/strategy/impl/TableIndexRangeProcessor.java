package org.kg.ctl.strategy.impl;

import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/23 3:19 PM
 */
public abstract class TableIndexRangeProcessor<T> extends AbstractTaskFromTo<T> {



    public TableIndexRangeProcessor(DbBatchQueryMapper<T> dbBatchQueryMapper) {
        super(dbBatchQueryMapper);
    }

    @Override
    protected void checkValid(TaskPo.InitialSnapShot taskSnapShot) {
        Assert.isTrue(Objects.nonNull(taskSnapShot.getTableStart()) && Objects.nonNull(taskSnapShot.getTableEnd())
                && powerOfTwoExponent(taskSnapShot.getDivideTableBatchSize()) > 0, "your choose table id with time range， but not support valid id range");
        Assert.isTrue(this.getBatchSize() <= 1000, "query  size :" + this.getBatchSize() + " too more， reject request");
    }

    @Override
    protected Function<TaskSegment, Boolean> buildExecuteFunction(TaskPo.InitialSnapShot initialSnapShot) {
        return (task) -> batchProcessWithIdRange(task.getSnapshotValue(), initialSnapShot.getTargetTime(), task.getStartTime(), task.getEndTime());
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        return initialSnapShot.getEndTime().isBefore(taskSegment.getEndTime().plusNanos(1)) && initialSnapShot.getTableEnd() <= (taskSegment.getEndIndex());
    }

    @Override
    protected void dynamicExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        // 分表以表纬度作为线程， 未分表以线程数为主
        int n = getConcurrentThreadCount();
        for (int i = 0; i < n; i++) {
            TaskSegment taskSegment = workingTaskSegment.get(i);
            executeTask(taskSegment, buildExecuteFunction(initialSnapShot));
        }
    }

    public static int powerOfTwoExponent(int number) {
        // 如果输入小于等于0或者不是2的幂次方，返回-1表示无效
        if (number <= 0 || (number & (number - 1)) != 0) {
            return -1;
        }
        int exponent = 0;
        while (number > 1) {
            number >>= 1;
            exponent++;
        }
        return exponent;
    }

    @Override
    protected void processTaskByScene(TaskPo taskJob, TaskPo.InitialSnapShot taskSnapShot) {
        int batchDivideTable = powerOfTwoExponent(taskSnapShot.getDivideTableBatchSize());
        int tempEnd = 0;
        int end = taskSnapShot.getTableEnd();
        int start = taskSnapShot.getTableStart();
        AtomicReference<TaskSegment> taskSegment = new AtomicReference<TaskSegment>();
        ExecutorService executorService = executorService();
        while (tempEnd < end) {
            tempEnd = start + batchDivideTable;
            if (tempEnd > end) {
                tempEnd = end;
            }
            CountDownLatch countDownLatch = new CountDownLatch(tempEnd - start + 1);
            for (int i = start; i <= tempEnd; i++) {
                int finalI = i;
                executorService.execute(() -> {
                    if (!isRun()) {
                        return;
                    }
                    String currentTableName = taskSnapShot.getIndex() + finalI;
                    taskSegment.set(splitAndRunTask(taskJob.getId(), taskSnapShot, currentTableName));
                    countDownLatch.countDown();
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            start = tempEnd + 1;
        }
        if (isRun() && Objects.nonNull(taskSegment.get())) {
            currentSceneLastSegemntMap.put(getTaskKey(taskJob), taskSegment.get());
        }

    }



}
