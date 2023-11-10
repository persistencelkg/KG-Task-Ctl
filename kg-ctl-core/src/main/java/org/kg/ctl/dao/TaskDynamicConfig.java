package org.kg.ctl.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.util.JsonUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/8/28 2:20 PM
 */

@Data
@Component
@Slf4j
public class TaskDynamicConfig {

    private static Map<String, TaskGranularConfig> jobConfig;

    @Value("${kg.job-config}")
    public void setJobConfig(String jobConfig) {
        TaskDynamicConfig.jobConfig = JsonUtil.toBean(jobConfig, new TypeReference<Map<String, TaskGranularConfig>>() {});
    }

    public static TaskGranularConfig getConfig(String str) {
        if (!jobConfig.containsKey(str)) {
            log.warn("not exist job config:{}", str);
            return new TaskGranularConfig();
        }
        return jobConfig.get(str);
    }
    
    public static Collection<TaskGranularConfig> getAll() {
        return jobConfig.values();
    }

    public static TaskGranularConfig getConfig(Class<?> str) {
        return jobConfig.get(str.getSimpleName());
    }

    public static void main(String[] args) {
        System.out.println(TaskGranularConfig.class.getSimpleName());
        System.out.println(JsonUtil.toJson(new TaskGranularConfig()));
    }
}
