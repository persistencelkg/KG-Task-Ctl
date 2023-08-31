package org.kg.ctl.dao.enums;


import lombok.AllArgsConstructor;
import lombok.Getter;
import org.kg.ctl.util.TaskUtil;

import java.text.MessageFormat;
import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务切分粒度
 *
 * @author likaiguang
 */

@Getter
@AllArgsConstructor
public enum TaskTimeSplitEnum {

    /**
     * Based on industrial experience, it can be seen that the granularity of single synchronization for most tasks is sufficient to meet the requirements of a day.
     * Coarse granularity is not a good design, such as memory performance
     */
    SECOND("second", "PT{0}S"),
    MINUTE("minute", "PT{0}M"),
    HOUR("hour", "PT{0}H"),
    DAY("day", "P{0}D");

    private final String granular;

    private final String period;

    private final static Map<String, String> MAP = new HashMap<>(4);

    static {
        TaskTimeSplitEnum[] values = TaskTimeSplitEnum.values();
        for (TaskTimeSplitEnum val : values) {
            MAP.put(val.getGranular(), val.getPeriod());
        }
    }

    public static TemporalAmount getDuration(String key, Integer val) {
        String s = MAP.get(key);
        if (s.contains("T")) {
            return TaskUtil.buildTaskDuration(MessageFormat.format(s, val));
        }
        return TaskUtil.buildTaskPeriod(MessageFormat.format(s, val));
    }

    public static void main(String[] args) {
        LocalTime of = LocalTime.of(23, 0, 0);
        LocalTime plus = of.plus(getDuration("second", 1));
        System.out.println(plus);

        LocalDate localDate = LocalDate.of(2023, 1, 1);
        Period day = Period.ofDays(1);
        System.out.println(localDate.plus(day));



        System.out.println(localDate.plus(getDuration("day", 1)));
    }

}

