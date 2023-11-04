package org.kg.ctl.strategy.impl;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @description: 业务id处理器
 * @author: 李开广
 * @date: 2023/5/23 4:07 PM
 */
@Slf4j
public abstract class BizIdProcessor<T> extends AbstractTaskFromTo<T> {


    public BizIdProcessor(DbBatchQueryMapper<T> dbBatchQueryMapper) {
        super(dbBatchQueryMapper);
    }

    @Override
    protected void checkValid(TaskPo.InitialSnapShot taskSnapShot) {
        Collection<?> dataList = taskSnapShot.getDataList();
        Assert.isTrue(!CollectionUtils.isEmpty(dataList), "you biz id to split task，but not support id data list ");
        Assert.isTrue(predicateValid().test(dataList), "not support biz id type:" + dataList.getClass());
    }

    private Predicate<Collection<?>> predicateValid() {
        return dataList -> dataList.getClass().isAssignableFrom(Number.class) || dataList.getClass().isAssignableFrom(CharSequence.class);
    }

//    @Override
//    public List<TaskSegment> splitTask(String taskId, TaskPo.InitialSnapShot initialSnapShot) {
//        // batchSize 和 线程数 互斥
//        Integer batchSize = this.getBatchSize();
//        ArrayList<?> dataList = new ArrayList<>(initialSnapShot.getDataList());
//        if (Objects.isNull(batchSize) || batchSize <= 0) {
//            Integer threadCount = this.getConcurrentThreadCount();
//            batchSize = dataList.size() / threadCount;
//        }
//        log.info("批量大小：{}", batchSize);
//        return TaskUtil.list(taskId, dataList, batchSize);
//    }


    @Override
    protected Function<TaskSegment, Boolean> doExecuteTask(TaskPo.InitialSnapShot initialSnapShot) {
        return (taskSegment) -> batchProcessWithBizIdList(initialSnapShot.getIndex(), initialSnapShot.getTargetBizId(), initialSnapShot.isDivideTable(), initialSnapShot.getDataList());
    }

    @Override
    protected boolean judgeTaskFinish(TaskPo.InitialSnapShot initialSnapShot, TaskSegment taskSegment) {
        ArrayList<?> dataList = new ArrayList<>(initialSnapShot.getDataList());
        Object lastElement = dataList.get(dataList.size() - 1);
        return lastElement.equals(dataList.get(taskSegment.getEndIndex()));
    }
}
