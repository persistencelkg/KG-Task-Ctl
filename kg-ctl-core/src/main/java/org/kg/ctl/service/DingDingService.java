package org.kg.ctl.service;



import org.kg.ctl.util.DingDingUtil;

import java.text.MessageFormat;

/**
 * @author likaiguang
 * @date 2023/4/21 3:40 下午
 */
public interface DingDingService {

    default String getDingUrl() {
        return null;
    }

    default String getDingSercet() {
        return null;
    }

    default String getEnv() {
        return "local";
    }

    default void dingInfoLog(String msg) {
        DingDingUtil.sendInfoMsg(MessageFormat.format("{0}|{1}", getEnv(), msg), getDingUrl(), getDingSercet());
    }

    default void dingErrorLog(String msg) {
        DingDingUtil.sendWarnMsg(MessageFormat.format("{0}|{1}", getEnv(), msg), getDingUrl(), getDingSercet());
    }
}
