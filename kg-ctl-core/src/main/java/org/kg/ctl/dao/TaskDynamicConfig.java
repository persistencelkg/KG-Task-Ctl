package org.kg.ctl.dao;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.config.JobConstants;
import org.kg.ctl.util.JsonUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/28 2:20 PM
 */

@Data
@Component
@Slf4j
public class TaskDynamicConfig {

    private static TaskDynamicConfig configInstance;
    private String[] bizPeekDuration = new String[]{"6->10", "11->14", "16->21"};
    /**
     * 低峰最大查询数据量
     */
    private int maxBatchSize = 500;

    /**
     * 高峰最大查询数据量
     */
    private int batchSize = 300;

    /**
     * 每次执行后的休眠时间 毫秒
     */
    private long sleepTime = 1000L;

    private int submitThreadCount = 2;

    /**
     * 业务场景
     */
    private String bizScene;

    private String url;
    private String secret;

    private Map<String, TaskGranularConfig> custom;

    @Value("${kg.job-config:}")
    public void setJobConfig(String jobConfig) {
        TaskDynamicConfig.configInstance = JsonUtil.toBean(jobConfig, TaskDynamicConfig.class);
        if (Objects.isNull(configInstance)) {
            throw new IllegalArgumentException("not config ctl job config： `kg.job-config`");
        }
    }

    public static TaskGranularConfig getConfig(String str) {
        Map<String, TaskGranularConfig> customConfig = configInstance.getCustom();
        if (Objects.isNull(customConfig) || !customConfig.containsKey(str)) {
            throw new IllegalArgumentException("not find ctl job key:" + str);
        }
        TaskDynamicConfig globalConfig = configInstance;
        TaskGranularConfig taskGranularConfig = customConfig.get(str);
        if (Objects.isNull(taskGranularConfig.getUrl())) {
            taskGranularConfig.setUrl(globalConfig.getUrl());
        }
        if (Objects.isNull(taskGranularConfig.getSecret())) {
            taskGranularConfig.setSecret(globalConfig.getSecret());
        }
        if (Objects.isNull(taskGranularConfig.getBizPeekDuration())) {
            taskGranularConfig.setBizPeekDuration(globalConfig.getBizPeekDuration());
        }
        int globalBatchSize = globalConfig.getBatchSize() <= 0 ? JobConstants.getBatchSize() : globalConfig.getBatchSize();
        long globalSleepTime = globalConfig.getSleepTime() <= 10 ? JobConstants.getSleepMillSecond() : globalConfig.getSleepTime();
        int globalMaxBatchSize = globalConfig.getMaxBatchSize() <= 0 ? globalBatchSize << 1 : globalConfig.getMaxBatchSize();
        if (taskGranularConfig.getBatchSize() <= 0) {
            taskGranularConfig.setBatchSize(globalBatchSize);
        }
        if (taskGranularConfig.getMaxBatchSize() <= 0) {
            taskGranularConfig.setMaxBatchSize(globalMaxBatchSize);
        }
        if (taskGranularConfig.getSleepTime() <= 0) {
            taskGranularConfig.setSleepTime(globalSleepTime);
        }
        if (taskGranularConfig.getSubmitThreadCount() <= 0) {
            taskGranularConfig.setSubmitThreadCount(globalConfig.getSubmitThreadCount());
        }
        return taskGranularConfig;
    }


    public static TaskDynamicConfig getAll() {
        return configInstance;
    }

    public static TaskGranularConfig getConfig(Class<?> str) {
        return getConfig(str.getSimpleName());
    }

    public static void main(String[] args) {
        TaskDynamicConfig taskDynamicConfig = new TaskDynamicConfig();
        HashMap<String, TaskGranularConfig> map = new HashMap<>();
        map.put("run", new TaskGranularConfig());
        taskDynamicConfig.setCustom(map);
        System.out.println(JsonUtil.toJson(taskDynamicConfig));
    }

}
