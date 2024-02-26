package org.kg.ctl.core;

import com.baomidou.mybatisplus.extension.service.IService;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Description: 业务id处理器
 * Author: 李开广
 * Date: 2023/5/23 4:07 PM
 */
@Slf4j
public abstract class UniqueKeyDataSyncProcessor<S, T> extends DataSyncCommonProcessor<S, T> {

    public UniqueKeyDataSyncProcessor(DbBatchQueryMapper<S> from, DbBatchQueryMapper<T> to, IService<T> iService) {
        super(from, to, iService);
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

    @Override
    protected Function<TaskSegment, Integer> buildExecuteFunction(TaskPo.InitialSnapShot initialSnapShot) {
        return (taskSegment) -> batchProcessWithBizIdList(initialSnapShot.getIndex(), initialSnapShot.getTargetBizId(), initialSnapShot.isDivideTable(), initialSnapShot.getDataList());
    }

    /**
     * key 为待分表，value 在改分表的数据
     *
     * @param data
     * @return
     */
    protected Map<String, ArrayList<Object>> getTableSuffixMap(String tablePrefix, Collection<?> data) {
        HashMap<String, ArrayList<Object>> map = new HashMap<>();
        for (Object value : data) {
            String logicTable = getLogicTable(value);
            if (Objects.isNull(logicTable)) {
                logicTable = tablePrefix;
            }
            map.computeIfAbsent(logicTable, ref -> new ArrayList<>()).add(value);
        }
        return map;
    }

    protected abstract String getLogicTable(Object value);


    protected final Integer batchProcessWithBizIdList(String fullTableName, String targetBizId, boolean isDivideTable, Collection<?> collection) {

        if (isDivideTable) {
            Map<String, ArrayList<Object>> tableSuffixMap = getTableSuffixMap(fullTableName, collection);
            for (String s : tableSuffixMap.keySet()) {
                Assert.isTrue(!Character.isDigit(s.charAt(s.length() - 1)), s + "is not a valid sub table");
            }
            tableSuffixMap.keySet().forEach(val -> {
                // 分表通过循环遍历
                List<S> sources = sourceDbBatchQueryMapper.selectListWithUniqueKeyList(val, targetBizId, tableSuffixMap.get(val));
                if (ObjectUtils.isEmpty(sources)) {
                    return;
                }
                batchProcessSourceData(sources, fullTableName, true);
            });
        } else {
            // 单表
            List<S> sources = sourceDbBatchQueryMapper.selectListWithUniqueKeyList(fullTableName, targetBizId, collection);
            if (ObjectUtils.isEmpty(sources)) {
                return 0;
            }
            batchProcessSourceData(sources, fullTableName, true);
        }
        return collection.size();
    }

}
