package org.kg.ctl.dao.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

/**
 * Description: 增量同步周期
 * Author: 李开广
 * Date: 2023/11/1 6:52 PM
 */
@AllArgsConstructor
@Getter
public enum IncrementSyncPeriod {
    T_MINUS_1(TimeUnit.DAYS.toSeconds(1)),
    T_0(TimeUnit.DAYS.toSeconds(0)),
    T_ADD_1(TimeUnit.DAYS.toSeconds(-1));

    private final long tn;

    public static long getSyncPeriodWithDuration(int n, TimeUnit unit) {
        return unit.toSeconds(n);
    }

    public static void main(String[] args) {
        System.out.println(IncrementSyncPeriod.T_0.getTn());
    }
}
