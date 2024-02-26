package org.kg.ctl.core;

import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.service.CheckService;
import org.kg.ctl.util.PerfUtil;
import org.kg.ctl.util.TaskUtil;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/22 10:02 AM
 */
public abstract class DataCheckProcessor<Source, Target> extends AbstractTaskFromTo<Source, Target> {

    @Resource
    private CheckService checkService;

    public DataCheckProcessor(DbBatchQueryMapper<Source> from, DbBatchQueryMapper<Target> target) {
        super(from, target);
    }


    @Override
    protected void batchProcessSourceData(Collection<Source> sourceData, String tableName, boolean insertCovert) {
        this.checkEachOther(sourceData, tableName, ignoredFields());
    }

    protected abstract Set<String> ignoredFields();

    protected void checkEachOther(Collection<Source> sourceData, String tableName, Set<String> ignoreFields) {
        String column = TaskUtil.camelToUnderLine(uniqueKey());
        String property = TaskUtil.underLineToCamel(uniqueKey());
        String keyPrefix = TaskUtil.getPrefixWithOutUnderLine(tableName);
        Map<Object, Source> dbMap = sourceData.stream().collect(Collectors.toMap(ref -> CheckService.getFieldValue(ref, ref.getClass(), property), ref -> ref));
        List<Target> tidbUniqueData = this.targetDbBatchQueryMapper.selectListWithUniqueKeyList(keyPrefix, column, dbMap.keySet());
        Map<Object, Target> tiDBMap = tidbUniqueData.stream().collect(Collectors.toMap(ref -> CheckService.getFieldValue(ref, ref.getClass(), property), ref -> ref));
        int checkConsistSize = sourceData.size();
        Set<Map.Entry<Object, Source>> dbEntry = dbMap.entrySet();
        TaskModeEnum taskModeEnum = TaskModeEnum.getInstance(getMode());
        List<Object> lostData = new ArrayList<>();
        for (Map.Entry<Object, Source> entry : dbEntry) {
            Target target = tiDBMap.get(entry.getKey());
            if (Objects.isNull(target)) {
                lostData.add(entry.getKey());
                continue;
            }
            boolean check = checkService.check(entry.getValue(), target, ignoreFields);
            if (!check) {
                checkConsistSize--;
                PerfUtil.countFail(keyPrefix, taskModeEnum, 1);
            }
        }
        if (!ObjectUtils.isEmpty(lostData)) {
            dingErrorLog(tableName + "同步数据缺失:" + lostData);
            PerfUtil.countFail(keyPrefix, taskModeEnum, lostData.size());
        }
        int notConsist = sourceData.size() - checkConsistSize;
        if (checkConsistSize < sourceData.size() && notConsist < 10) {
            dingErrorLog(tableName + "字段比对不一致请关注比对日志, 不一致数量：" + notConsist);
        }
        PerfUtil.countSuccess(keyPrefix, taskModeEnum, checkConsistSize);
    }


}
