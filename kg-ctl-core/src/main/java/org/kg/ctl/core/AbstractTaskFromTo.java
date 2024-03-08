package org.kg.ctl.core;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.IdRange;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.mapper.SyncMapper;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ObjectUtils;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.kg.ctl.config.JobConstants.QUERY_SIZE_PER_SEC;

/**
 * Description: 基础批量查询处理
 * Author: 李开广
 * Date: 2023/5/25 3:23 PM
 */
@Slf4j
public abstract class AbstractTaskFromTo<Source, Target> extends AbstractTaskContext {

    protected final SyncMapper<Source> sourceDbBatchQueryMapper;
    protected final SyncMapper<Target> targetDbBatchQueryMapper;


    @Autowired
    public AbstractTaskFromTo(SyncMapper<Source> from, SyncMapper<Target> targetDbBatchQueryMapper) {
        this.sourceDbBatchQueryMapper = from;
        this.targetDbBatchQueryMapper = targetDbBatchQueryMapper;
    }



    /**
     * 默认都是有时间范围
     *
     * @param initialSnapShot
     * @param taskSegment
     * @return
     */
    protected final int batchProcessWithIdRange(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        String fullTableName = getFullTableName(taskSegment, initialSnapShot);
        String targetTime = initialSnapShot.getTargetTime();
        LocalDateTime startTime = taskSegment.getStartTime();
        LocalDateTime endTime = taskSegment.getEndTime();
        int batchSize = this.getDynamicDbQueryNumber();
        IdRange idRange = sourceDbBatchQueryMapper.queryMinIdWithTime(fullTableName, targetTime, taskSegment.getStartTime(), taskSegment.getEndTime());
        if (Objects.isNull(idRange)) {
            log.warn("batchProcessWithIdRange fullTableName:{}, current time :{}-{} not have data", fullTableName, DateTimeUtil.format(startTime), DateTimeUtil.format(endTime));
            return 0;
        }
        //log.info("batchProcessWithIdRange fullTableName:{}, current process time :{}-{} size: {}", fullTableName, DateTimeUtil.format(startTime), DateTimeUtil.format(endTime), batchSize);
        Long minId = idRange.getMinId();
        Long maxId = idRange.getMaxId();
        long tmp;
        AtomicInteger atomicLong = new AtomicInteger();
        while (minId <= maxId) {
            if (!isRun()) {
                return 0;
            }
            tmp = minId + batchSize;
            if (tmp > maxId) {
                tmp = maxId;
            }
            List<Source> ts = sourceDbBatchQueryMapper.selectListWithTableIdAndTimeRange(fullTableName, minId, tmp, targetTime, startTime, endTime, batchSize);
            if (!ObjectUtils.isEmpty(ts)) {
                batchProcessSourceData(ts, fullTableName, initialSnapShot.isInsertCover());
                try {
                    TimeUnit.MILLISECONDS.sleep(this.getSleepTime());
                } catch (InterruptedException ignored) {
                }
                atomicLong.getAndAdd(ts.size());
            }
            minId = tmp + 1;

        }
        return atomicLong.get();
    }

    protected final int batchProcessWithIdNotContinuous(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        String fullTableName = getFullTableName(taskSegment, initialSnapShot);
        String targetTime = initialSnapShot.getTargetTime();
        LocalDateTime startTime = taskSegment.getStartTime();
        LocalDateTime endTime = taskSegment.getEndTime();
        // 按时间切片
        List<Source> ts = sourceDbBatchQueryMapper.selectListWithTimeRange(fullTableName, targetTime, startTime, endTime);
        if (ObjectUtils.isEmpty(ts)) {
            log.warn("batchProcessWithOutIdRange fullTableName:{}, current process time :{}-{} not data", fullTableName, DateTimeUtil.format(startTime), DateTimeUtil.format(endTime));
            return 0;
        }
        //log.info("batchProcessWithOutIdRange fullTableName:{}, current process time :{}-{} size:{}", fullTableName, DateTimeUtil.format(startTime), DateTimeUtil.format(endTime), ts.size());
        batchProcessSourceData(ts, fullTableName, initialSnapShot.isInsertCover());
        try {
            TimeUnit.MILLISECONDS.sleep(this.getSleepTime());
        } catch (InterruptedException ignored) {
        }
        return ts.size();
    }

    /**
     * 使用者只需要实现该业务逻辑即可
     */
    protected abstract void batchProcessSourceData(Collection<Source> sourceData, String tableName, boolean insertCovert);

    @Override
    protected void processSimpleTableTask(TaskPo.InitialSnapShot initialSnapShot, String tableId) {
        TemporalAmount duration = isOnline() ? TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval()) : TaskUtil.buildDurationPeriod(initialSnapShot.getSyncInterval());
        ArrayList<TaskSegment> objects = new ArrayList<>();
        LocalDateTime tempStart = initialSnapShot.getStartTime();
        LocalDateTime tempEnd;
        // try to allocate simple time range
        tryToAllocateSimpleTimeRange(tempStart, tempStart.plus(duration), tableId, initialSnapShot.getTargetTime());
        int i = 0;
        TaskSegment build = null;
        while (isRun()) {
            tempEnd = tempStart.plus(duration);
            if (tempStart.plusNanos(1).isAfter(initialSnapShot.getEndTime())) {
                break;
            }
            build = buildTaskSegment(++i, tempStart, tempEnd, tableId, initialSnapShot.getMode());
            objects.add(build);
            if (objects.size() % getTaskSubmitCount() == 0) {
                batchExecute(objects, initialSnapShot);
                String partInfo = getPartInfo(build, MessageFormat.format("{0} current schedule：{1} %", tableId, compute(initialSnapShot, i)), false);
                log.info(partInfo);
                objects.clear();
            }
            tempStart = tempEnd;
        }
        if (isRun() && objects.size() > 0) {
            String partInfo = getPartInfo(build, MessageFormat.format("{0} current schedule：{1} %", tableId, compute(initialSnapShot, i)), false);
            log.info(partInfo);
            batchExecute(objects, initialSnapShot);
        }
    }

    private void tryToAllocateSimpleTimeRange(LocalDateTime tempStart, LocalDateTime end, String tableId, String targetTime) {
        if (idIncrement()) {
            return;
        }
        // todo选择一个高峰期
        long start = System.currentTimeMillis();
        Integer integer = sourceDbBatchQueryMapper.selectCountWithTimeRange(tableId, targetTime, tempStart, end);
        long duration = (System.currentTimeMillis() - start) / 1000;
        if (Objects.nonNull(integer) && integer > QUERY_SIZE_PER_SEC) {
            dingErrorLog(MessageFormat.format("{0}|current time interval too large，{} - {} query too much result set：{1}", getTaskId(), integer));
        }
        if (duration > 2) {
            dingErrorLog(MessageFormat.format("{0}| current table:{1}, time field:{2}  may be lack in key_index, execute cost time:{3}s", getTaskId(), tableId, targetTime, duration));
        }
    }


    public TaskSegment buildTaskSegment(int i, LocalDateTime tempStart, LocalDateTime tempEnd, String tableId, String mode) {
        TaskSegment build = TaskSegment.builder()
                .segmentId(i)
                .status(TaskStatusEnum.WORKING.getCode())
                .startTime(tempStart)
                .endTime(tempEnd)
                .mode(mode)
                .build();
        if (Objects.nonNull(tableId)) {
            build.setSnapshotValue(tableId);
        }
        return build;
    }



    protected String getFullTableName(TaskSegment taskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        boolean divideTable = initialSnapShot.isDivideTable();
        if (divideTable) {
            return taskSegment.getSnapshotValue();
        } else {
            return initialSnapShot.getIndex();
        }
    }


    protected Function<TaskSegment, Integer> buildExecuteFunction(TaskPo.InitialSnapShot initialSnapShot) {
        if (!idIncrement()) {
            return (taskSegment) -> batchProcessWithIdNotContinuous(initialSnapShot, taskSegment);
        }
        return (taskSegment) -> batchProcessWithIdRange(initialSnapShot, taskSegment);
    }

    @Override
    protected void batchExecute(List<TaskSegment> objects, TaskPo.InitialSnapShot initialSnapShot) {
        TaskSegment stopSegment;
        if (initialSnapShot.isDivideTable()) {
            stopSegment = divideTableExecuteTask(objects, initialSnapShot);
        } else {
            stopSegment = simpleTableExecuteTask(objects, initialSnapShot);
        }
        stopSegment = Objects.isNull(stopSegment) ? objects.get(0) : stopSegment;
        if (!isRun()) {
            dingErrorLog(getPartInfo(stopSegment, "任务手动停止!", true));
        }
        objects.clear();
    }


    protected TaskSegment divideTableExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        for (TaskSegment taskSegment : workingTaskSegment) {
            doExecuteTask(taskSegment, buildExecuteFunction(initialSnapShot));
            if (!isRun()) {
                return taskSegment;
            }
        }
        return null;
    }


    protected TaskSegment simpleTableExecuteTask(List<TaskSegment> workingTaskSegment, TaskPo.InitialSnapShot initialSnapShot) {
        int n = Math.min(getTaskSubmitCount(), workingTaskSegment.size());
        ExecutorService executorService = executorService();
        int batchCount = workingTaskSegment.size() % n == 0 ? workingTaskSegment.size() / n : workingTaskSegment.size() / n + 1;
        for (int p = 0; p < batchCount; p++) {
            List<TaskSegment> taskSegments;
            if (p == batchCount - 1) {
                taskSegments = workingTaskSegment.subList(n * p, workingTaskSegment.size());
            } else {
                taskSegments = workingTaskSegment.subList(n * p, n * p + p);
            }
            CountDownLatch countDownLatch = new CountDownLatch(n);
            for (int i = 0; i < n; i++) {
                TaskSegment taskSegment = taskSegments.get(i);
                executorService.execute(() -> {
                    try {
                        doExecuteTask(taskSegment, buildExecuteFunction(initialSnapShot));
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
                if (!isRun()) {
                    return workingTaskSegment.get(0);
                }
            }

            try {
                countDownLatch.await();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        return null;
    }

}
