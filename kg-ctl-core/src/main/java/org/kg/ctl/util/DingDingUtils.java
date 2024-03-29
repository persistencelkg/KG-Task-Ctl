package org.kg.ctl.util;

import lombok.extern.slf4j.Slf4j;
import org.lkg.ding.DingDingMsg;
import org.lkg.ding.DingDingUtil;
import org.springframework.util.ObjectUtils;

import java.text.MessageFormat;

/**
 * Author 李开广
 * Date 2023/2/22 4:29 下午
 */
@Slf4j
public class DingDingUtils {
    private static final String INFO_COLOR = "#111000";
    private static final String ERROR_COLOR = "#ff6666";


    private static void sendAndPrintMsg(String phone, String msg, String url, String secret, String color) {
//        try {
//            DingDingUtils.sendMessage(DingDingMsg.createMarkdown(phone, "--", MessageFormat.format("<font color=\"{0}\">{1}</font>", color, msg)), url, secret);
//        } catch (Exception ignored) {}
        DingDingUtil.sendMessage(DingDingMsg.createMarkDown("--", MessageFormat.format("<font color=\"{0}\">{1}</font>", color, msg)), url, secret, false, phone);
    }


    public static void sendInfoMsg(String msg, String url, String secret) {
        log.info(msg);
        if (ObjectUtils.isEmpty(url)) {
            return;
        }
        sendAndPrintMsg("", msg, url, secret, INFO_COLOR);
    }

    public static void sendWarnMsg(String msg, String url, String secret) {
        log.warn(msg);
        sendAndPrintMsg("", msg, url, secret, ERROR_COLOR);
    }
}
