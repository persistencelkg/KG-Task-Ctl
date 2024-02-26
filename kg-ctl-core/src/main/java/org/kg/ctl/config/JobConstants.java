package org.kg.ctl.config;

/**
 * Description: 常量配置
 * Author: 李开广
 * Date: 2023/5/23 2:39 PM
 */
public class JobConstants {

    public final static String LINE = "->";

    public final static String IN = ",";

    public final static int QUERY_SIZE_PER_SEC = 1000;

    public final static int INTERNAL_PROCESSORS = Runtime.getRuntime().availableProcessors();


    public static int getBatchSize() {
        return QUERY_SIZE_PER_SEC / INTERNAL_PROCESSORS;
    }

    public static int getSleepMillSecond() {
        return getBatchSize();
    }

}
