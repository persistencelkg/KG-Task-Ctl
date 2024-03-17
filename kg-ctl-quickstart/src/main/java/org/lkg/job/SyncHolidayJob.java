package org.lkg.job;

import com.baomidou.mybatisplus.extension.service.IService;
import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.kg.ctl.core.DataSyncCommonProcessor;
import org.kg.ctl.mapper.DbBatchQueryMapper;
import org.kg.ctl.mapper.SyncMapper;
import org.lkg.pojo.QcHolidayDict;
import org.lkg.pojo.QcHolidayTargetDict;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/27 4:44 PM
 */
@Component
public class SyncHolidayJob extends DataSyncCommonProcessor<QcHolidayDict, QcHolidayTargetDict> {

    public SyncHolidayJob(SyncMapper<QcHolidayDict> from, SyncMapper<QcHolidayTargetDict> target, IService<QcHolidayTargetDict> iService) {
        super(from, target, iService);
    }

    @XxlJob("SyncHolidayJob")
    public boolean run() {
        super.runTask();
        return true;
    }


    @Override
    protected List<QcHolidayTargetDict> convertToTargetObject(Collection<QcHolidayDict> sourceData, String tableName) {
        ArrayList<QcHolidayTargetDict> list = new ArrayList<>();
        for (QcHolidayDict sourceDatum : sourceData) {
            QcHolidayTargetDict qcHolidayTargetDict = new QcHolidayTargetDict();
            BeanUtils.copyProperties(sourceDatum, qcHolidayTargetDict);
            list.add(qcHolidayTargetDict);
        }
        return list;
    }

    @Override
    protected String targetTableName(String tableName) {
        return "qc_holiday_target_dict";
    }

    @Override
    public String uniqueKey() {
        return "id";
    }

    /**
     * 测试使用，非必须，默认xxlJobHelper.getParam();
     *
     * @return
     */
    @Override
    public String getParam() {
        return "{\"targetTime\":\"update_time\",\"startTime\":\"2022-09-01 00:00:00\",\"endTime\":\"2024-01-01 23:59:59\",\"tablePreFix\":\"qc_holiday_dict\",\"mode\":\"full_sync\",\"syncInterval\":\"PT1H\",\"syncPeriod\":\"T-1\"}";
    }
}
