package org.kg.ctl.core;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.IdRange;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskStatusEnum;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.util.DateTimeUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Description: 基础批量查询处理
 * Author: 李开广
 * Date: 2023/5/25 3:23 PM
 */
@Slf4j
public abstract class AbstractTaskFromTo<Source> extends AbstractTaskContext {

    private final DbBatchQueryMapper<Source> dbBatchQueryMapper;


    public AbstractTaskFromTo(DbBatchQueryMapper<Source> dbBatchQueryMapper) {
        this.dbBatchQueryMapper = dbBatchQueryMapper;
    }

    /**
     * key 为待分表，value 在改分表的数据
     *
     * @param data
     * @return
     */
    protected Map<String, Collection<?>> getTableSuffixMap(String tablePrefix, Collection<?> data) {
        return null;
    }


    protected final boolean batchProcessWithBizIdList(String fullTableName, String targetBizId, boolean isDivideTable, Collection<?> collection) {

        if (isDivideTable) {
            Map<String, Collection<?>> tableSuffixMap = getTableSuffixMap(fullTableName, collection);
            Assert.isTrue(!CollectionUtils.isEmpty(tableSuffixMap), fullTableName + "exist sub tables，but not found");
            for (String s : tableSuffixMap.keySet()) {
                Assert.isTrue(!Character.isDigit(s.charAt(s.length() - 1)), s + "is not a valid sub table");
            }
            tableSuffixMap.keySet().forEach(val -> {
                // 分表通过循环遍历
                List<Source> sources = dbBatchQueryMapper.selectListWithBizIdList(val, targetBizId, collection);
                batchToTarget(sources);
            });
        } else {
            // 单表
            List<Source> sources = dbBatchQueryMapper.selectListWithBizIdList(fullTableName, targetBizId, collection);
            batchToTarget(sources);
        }
        return true;
    }


    protected final boolean batchProcessWithIdRange(String fullTableName, String targetTime, LocalDateTime startTime, LocalDateTime endTime) {
        int batchSize = this.getBatchSize();
        IdRange idRange = dbBatchQueryMapper.queryMinIdWithTime(fullTableName, targetTime, startTime, endTime);
        if (Objects.isNull(idRange)) {
            log.warn("current time :{}-{} not have data", DateTimeUtil.format(startTime), DateTimeUtil.format(endTime));
            return true;
        }
        Long minId = idRange.getMinId();
        Long maxId = idRange.getMaxId();
        long tmp;
        while (minId < maxId) {
            if (!isRun()) {
                return false;
            }
            tmp = minId + batchSize;
            if (tmp > maxId) {
                tmp = maxId;
            }
            List<Source> ts = dbBatchQueryMapper.selectListWithTableIdAndTimeRange(fullTableName, minId, tmp, batchSize, targetTime, startTime, endTime);
            batchToTarget(ts);
            try {
                TimeUnit.MILLISECONDS.sleep(this.getSleepTime());
            } catch (InterruptedException ignored) {}
            minId = tmp + 1;
        }
        return true;
    }

    /**
     * 使用者只需要实现该业务逻辑即可
     */
    protected abstract void batchToTarget(Collection<Source> sourceData);


    @Override
    protected TaskSegment splitAndRunTask(Integer taskId, TaskPo.InitialSnapShot initialSnapShot, String tableId) {
        TemporalAmount duration = TaskUtil.buildTaskDuration(initialSnapShot.getSyncInterval());
        ArrayList<TaskSegment> objects = new ArrayList<>();
        LocalDateTime tempStart = initialSnapShot.getStartTime();
        LocalDateTime tempEnd;
        TaskSegment lastSegment = null;
        int i = 0;
        while (isRun()) {
            tempEnd = tempStart.plus(duration);
            if (tempStart.plusNanos(1).isAfter(initialSnapShot.getEndTime())) {
                break;
            }

            TaskSegment build = TaskSegment.builder()
                    .taskId(taskId)
                    .segmentId(++i)
                    .status(TaskStatusEnum.WORKING.getCode())
                    .startTime(tempStart)
                    .endTime(tempEnd)
                    .build();
            if (Objects.nonNull(tableId)) {
                build.setSnapshotValue(tableId);
            }
            objects.add(build);
            if (objects.size() % getConcurrentThreadCount() == 0) {
                getTaskHandler().saveSegment(objects);
                dynamicExecuteTask(objects, initialSnapShot);
                lastSegment = objects.get(objects.size() - 1);
                objects.clear();
            }
            tempStart = tempEnd;
        }
        if (objects.size() > 0) {
            getTaskHandler().saveSegment(objects);
            dynamicExecuteTask(objects, initialSnapShot);
            lastSegment = objects.get(objects.size() - 1);
        }
        return lastSegment;
    }

    public static void main(String[] args) {
        Collection cl = new ArrayList<String>() {{
            add("2");
            add("3");
        }};
        System.out.println(String.join(JobConstants.LINE, cl));
    }

}
