package org.kg.ctl.config;

import org.kg.ctl.strategy.ThreadCountStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/11/10 2:31 PM
 */
@Configuration
public class CtlTaskThreadTaskConfiguration {

    public static final String IO_EXECUTOR = "ioTaskService";

    public static final String MIX_EXECUTOR = "mixTaskService";



    @Bean(name = IO_EXECUTOR)
    public ExecutorService ioTaskService(ThreadPoolConfig.CustomExecutorServiceFactory customExecutorServiceFactory) {
        return customExecutorServiceFactory.defaultCreate("IO-ctl-task",
                ThreadCountStrategy.INTERNAL_PROCESSORS << 1 + ThreadCountStrategy.INTERNAL_PROCESSORS,
                ThreadCountStrategy.INTERNAL_PROCESSORS << 1, (int) Math.min(300, Math.pow(ThreadCountStrategy.INTERNAL_PROCESSORS, 2)));
    }


    @Bean(name = MIX_EXECUTOR)
    public ExecutorService mixTaskService(ThreadPoolConfig.CustomExecutorServiceFactory customExecutorServiceFactory) {
        return customExecutorServiceFactory.defaultCreate("MIX-ctl-task",
                ThreadCountStrategy.INTERNAL_PROCESSORS << 1,
                ThreadCountStrategy.INTERNAL_PROCESSORS, (int) Math.min(800, Math.pow(ThreadCountStrategy.INTERNAL_PROCESSORS, 3)));
    }



}
