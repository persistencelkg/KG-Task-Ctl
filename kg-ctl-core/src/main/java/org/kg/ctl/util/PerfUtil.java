package org.kg.ctl.util;

import io.micrometer.core.instrument.Metrics;
import org.kg.ctl.dao.enums.TaskModeEnum;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/12/27 3:33 PM
 */
public class PerfUtil {

    private static final String DATA_HARMONY_SYSTEM_NAME = "data_harmony_name";
    private static final String DATA_HARMONY_SUC_NAME = "data_harmony_suc_tag";
    private static final String DATA_HARMONY_FAIL_NAME = "data_harmony_fail_tag";


    public static void countSuccess(String taskId, TaskModeEnum taskModeEnum, int size) {
        Metrics.counter(DATA_HARMONY_SYSTEM_NAME, DATA_HARMONY_SUC_NAME, taskId + taskModeEnum.getDescription()).increment(Math.max(size, 1));
    }

    public static void countFail(String taskId, TaskModeEnum taskModeEnum, int size) {
        Metrics.counter(DATA_HARMONY_SYSTEM_NAME, DATA_HARMONY_FAIL_NAME,  taskId + taskModeEnum.getDescription()).increment(Math.max(size, 1));
    }
}
