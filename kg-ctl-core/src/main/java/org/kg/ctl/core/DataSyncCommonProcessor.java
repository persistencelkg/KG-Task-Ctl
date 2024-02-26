package org.kg.ctl.core;

import com.baomidou.mybatisplus.extension.service.IService;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.service.CheckService;
import org.kg.ctl.util.TaskUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/21 7:13 PM
 */
@Slf4j
public abstract class DataSyncCommonProcessor<Source, Target> extends AbstractTaskFromTo<Source, Target> {


    private final IService<Target> iService;

    @Autowired
    public DataSyncCommonProcessor(DbBatchQueryMapper<Source> from, DbBatchQueryMapper<Target> target, IService<Target> iService) {
        super(from, target);
        this.iService = iService;
    }

    protected abstract List<Target> convertToTargetObject(Collection<Source> sourceData, String tableName);

    @Override
    protected void batchProcessSourceData(Collection<Source> sourceData, String tableName, boolean insertCovert) {
        List<Target> objects = convertToTargetObject(sourceData, tableName);
        Assert.isTrue(!CollectionUtils.isEmpty(objects), "your destination data is Empty!, you need supply the logic in method `convertToTargetObject` ");
        String uniqueKey = uniqueKey();
        String column = TaskUtil.camelToUnderLine(uniqueKey);
        String property = TaskUtil.underLineToCamel(uniqueKey);
        String keyPrefix = TaskUtil.getPrefixWithOutUnderLine(tableName);
        List<Object> dbUniqueKeyList = objects.stream().map(ref -> CheckService.getFieldValue(ref, ref.getClass(), property)).collect(Collectors.toList());
        List<Target> tidbUniqueData = this.targetDbBatchQueryMapper.selectListWithUniqueKeyList(keyPrefix, column, dbUniqueKeyList);

        List<Object> tidbUniqueList = tidbUniqueData.stream().map(ref -> CheckService.getFieldValue(ref, ref.getClass(), property)).collect(Collectors.toList());
        // need covert if necessary
        List<Target> needCover = objects.stream().filter(ref -> tidbUniqueList.contains(CheckService.getFieldValue(ref, ref.getClass(), property))).collect(Collectors.toList());
        if (ObjectUtils.isEmpty(needCover)) {
            LocalDateTime start = LocalDateTime.now();
            iService.saveBatch(objects);
            LocalDateTime end = LocalDateTime.now();
            log.info("{} batch add. size:{} cost:{}ms", keyPrefix, objects.size(), Duration.between(start, end).toMillis());
            return;
        }
        objects.removeIf(ref -> tidbUniqueList.contains(CheckService.getFieldValue(ref, ref.getClass(), property)));
        // no have insert
        if (!ObjectUtils.isEmpty(objects)) {
            LocalDateTime start = LocalDateTime.now();
            iService.saveBatch(objects);
            LocalDateTime end = LocalDateTime.now();
            log.info("{} batch add. size:{} cost:{}ms", keyPrefix, objects.size(), Duration.between(start, end).toMillis());
        }
        // if db have cover
        if (insertCovert) {
            LocalDateTime start = LocalDateTime.now();
            iService.updateBatchById(needCover);
            LocalDateTime end = LocalDateTime.now();
            log.info("{} batch update. size:{} cost:{}ms", keyPrefix, needCover.size(), Duration.between(start, end).toMillis());
        } else {
            // ignore if db have
        }
    }
}
