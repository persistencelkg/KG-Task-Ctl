package org.lkg.job;

import com.xxl.job.core.handler.annotation.XxlJob;
import org.kg.ctl.core.DataCheckProcessor;
import org.kg.ctl.mapper.SyncMapper;
import org.lkg.pojo.QcHolidayDict;
import org.lkg.pojo.QcHolidayTargetDict;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/3/16 9:41 PM
 */
@Component
public class CheckHolidayJob extends DataCheckProcessor<QcHolidayDict, QcHolidayTargetDict> {

    @XxlJob("CheckHolidayJob")
    public void checkHolidayJob() {
        super.runTask();
    }

    public CheckHolidayJob(SyncMapper<QcHolidayDict> from, SyncMapper<QcHolidayTargetDict> target) {
        super(from, target);
    }

    @Override
    protected Set<String> ignoredFields() {
        HashSet<String> objects = new HashSet<>();
        objects.add("id");
        return objects;
    }

    @Override
    public String uniqueKey() {
        return "id";
    }
}
