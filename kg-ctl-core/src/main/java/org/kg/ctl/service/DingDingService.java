package org.kg.ctl.service;



import org.kg.ctl.dao.TaskDynamicConfig;
import org.kg.ctl.dao.TaskPo;
import org.kg.ctl.dao.TaskSegment;
import org.kg.ctl.dao.enums.TaskModeEnum;
import org.kg.ctl.util.DingDingUtil;

import java.text.MessageFormat;

/**
 * Author 李开广
 * Date 2023/4/21 3:40 下午
 */

public interface DingDingService {


    default String getDingUrl() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getUrl();
    }

    default String getDingSecret() {
        return TaskDynamicConfig.getConfig(this.getClass().getSimpleName()).getSecret();
    }

    default String getEnv() {
        return "local";
    }

    default void dingInfoLog(String msg) {
        DingDingUtil.sendInfoMsg(MessageFormat.format("{0}|{1}", getEnv(), msg), getDingUrl(), getDingSecret());
    }

    default void dingErrorLog(String msg) {
        DingDingUtil.sendWarnMsg(MessageFormat.format("{0}|{1}", getEnv(), msg), getDingUrl(), getDingSecret());
    }

    default String getPartInfo(TaskSegment taskSegment, String msg, boolean hasEnv) {
        if (hasEnv) {
            return MessageFormat.format("{0}, current exec part task info:[{1}]|[{2}]", msg, taskSegment.getSnapshotValue(), taskSegment.getTimeRange());
        }
        return MessageFormat.format("{0}|{1}, current exec part task info:[{2}]|[{3}]", getEnv(), msg, taskSegment.getSnapshotValue(), taskSegment.getTimeRange());
    }

    default String getGlobalInfo(TaskPo.InitialSnapShot initialSnapShot, String msg) {
        return MessageFormat.format("{0}|{1}, global task info:[{2}]|[{3}]", TaskModeEnum.getInstance(initialSnapShot.getMode()).getDescription(),  msg, initialSnapShot.getIndex(), initialSnapShot.getTimeRange());
    }



}
