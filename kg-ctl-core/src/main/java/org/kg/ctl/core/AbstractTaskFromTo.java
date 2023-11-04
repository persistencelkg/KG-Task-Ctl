package org.kg.ctl.core;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.dao.IdRange;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.mapper.IdRangeMapper;
import org.kg.ctl.util.DateTimeUtil;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @description: 基础批量查询处理
 * @author: 李开广
 * @date: 2023/5/25 3:23 PM
 */
@Slf4j
public abstract class AbstractTaskFromTo<Source> extends AbstractTaskDispatcher {

    private final DbBatchQueryMapper<Source> dbBatchQueryMapper;

    @Resource
    private IdRangeMapper idRangeMapper;


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
        IdRange idRange = idRangeMapper.queryMinIdWithTime(fullTableName, targetTime, startTime, endTime);
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
            } catch (InterruptedException ignored) {
            }
            minId = tmp + 1;
        }
        return true;
    }

    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (taskSegment) ->
                splitTaskWithIdRange(initialSnapShot.getIndex(), initialSnapShot.getTargetTime(),
                        taskSegment.getStartTime(), taskSegment.getEndTime());
    }



    protected boolean splitTaskWithIdRange(String tableName, String targetTime, LocalDateTime start, LocalDateTime end) {
        // 是否是分表
        return batchProcessWithIdRange(tableName, targetTime, start, end);
//        if (Objects.isNull(tableStart) || Objects.isNull(tableEnd) || tableStart > tableEnd) {
//
//        }
//        ExecutorService executorService = executorService();
//        CountDownLatch countDownLatch = new CountDownLatch(tableEnd - tableStart + 1);
//        for (int i = tableStart; i <= tableEnd; i++) {
//            int finalI = i;
//            if (!isRun()) {
//                log.info("手动暂停，time range:{}-{},table range:{}-{}, current table index:{}",
//                        DateTimeUtil.format(start), DateTimeUtil.format(end), tableStart, tableEnd, finalI);
//                return false;
//            }
//            executorService.execute(() -> {
//                batchProcessWithIdRange(tableName + finalI, targetTime, start, end);
//                countDownLatch.countDown();
//            });
//        }
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException ignored) {
//        }
//        return true;
    }

    /**
     * 使用者只需要实现该业务逻辑即可
     */
    protected abstract void batchToTarget(Collection<Source> sourceData);


    public static void main(String[] args) {
        Collection cl = new ArrayList<String>() {{
            add("2");
            add("3");
        }};
        System.out.println(String.join(JobConstants.LINE, cl));
    }

}
