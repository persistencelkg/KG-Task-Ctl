package org.kg.ctl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.concurrent.ExecutorService;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/11/10 2:31 PM
 */
@Configuration
public class CtlTaskThreadTaskConfiguration {

    public static final String IO_TASK = "ioTask";



    @Lazy
    @Bean(IO_TASK)
    public ExecutorService ioTask(ThreadPoolConfig.CustomExecutorServiceFactory customExecutorServiceFactory) {
        return customExecutorServiceFactory.defaultCreate("IO-TASK-",
                JobConstants.INTERNAL_PROCESSORS,
                JobConstants.INTERNAL_PROCESSORS << 1, 100);
    }




}
