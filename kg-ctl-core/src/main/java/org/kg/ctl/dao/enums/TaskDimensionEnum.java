package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.kg.ctl.dao.TaskPo;
import org.springframework.util.ObjectUtils;

@Getter
@AllArgsConstructor
public enum TaskDimensionEnum {

    /**
     * 记录任务纬度：0：默认执行1次  1.按业务id执行任务  2.按时间段  3.时间段+索引 具体见DataSourceEnum
     */
    BIZ_ID(1),
    TIME_RANGE(2),
    TIME_RANGE_WITH_INDEX(3);



    private final Integer dimension;

    public static boolean isTimeRange(TaskDimensionEnum instance){
        return TaskDimensionEnum.TIME_RANGE_WITH_INDEX == instance || TaskDimensionEnum.TIME_RANGE == instance;
    }

    public static TaskDimensionEnum getInstance(TaskPo.InitialSnapShot taskSnapShot) {
        boolean bizId = !ObjectUtils.isEmpty(taskSnapShot.getDataList());
        boolean timeRange = !ObjectUtils.isEmpty(taskSnapShot.getSyncInterval())
                            && !ObjectUtils.isEmpty(taskSnapShot.getMode()) ;
        boolean tableIndex = !ObjectUtils.isEmpty(taskSnapShot.getMinId()) && !ObjectUtils.isEmpty(taskSnapShot.getMaxId());
        if (bizId) {
           return BIZ_ID;
        } else if (tableIndex && timeRange) {
            return TIME_RANGE_WITH_INDEX;
        } else if (timeRange) {
           return TIME_RANGE;
        }
        return null;

    }
}
