package org.kg.ctl.config;

import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 自定义线程池
 * Author 李开广
 * Date 2023/2/12 8:48 下午
 */
@Configuration
public class ThreadPoolConfig {

    static class MDCTaskDecorator implements TaskDecorator {

        @Override
        public Runnable decorate(Runnable runnable) {

            Map<String, String> contextMap = MDC.getCopyOfContextMap();

            return () -> {
                try {
                    if (contextMap != null) {
                        MDC.setContextMap(contextMap);
                    }
                    runnable.run();

                } finally {
                    if (MDC.getMDCAdapter() != null) {
                        MDC.clear();
                    }
                }
            };

        }
    }

    interface CustomExecutorServiceFactory {
        /**
         * @return @link CustomExecutorServiceFactory
         */
        ExecutorService create(String prefix, int maxThread, int coreSize, int queueSize, RejectedExecutionHandler rejectedExecutionHandler);

        /**
         * 创建默认调用者的线程
         *
         * @param prefix
         * @param maxThread
         * @param coreSize
         * @param queueSize
         * @return
         */
        ExecutorService defaultCreate(String prefix, int coreSize, int maxThread, int queueSize);
    }

    @Bean
    @Lazy
    public CustomExecutorServiceFactory customExecutorServiceFactory() {
        return new CustomExecutorServiceFactory() {
            @Override
            public ExecutorService create(String prefix, int maxThread, int coreSize, int queueSize, RejectedExecutionHandler rejectedExecutionHandler) {
                ThreadPoolTaskExecutor executorService = new ThreadPoolTaskExecutor();
                executorService.setBeanName(prefix);
                executorService.setThreadNamePrefix(prefix);
                executorService.setCorePoolSize(coreSize);
                executorService.setMaxPoolSize(maxThread);
                executorService.setQueueCapacity(queueSize);
                executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
                executorService.setWaitForTasksToCompleteOnShutdown(true);
                executorService.setTaskDecorator(new MDCTaskDecorator());
                // 初始化
                executorService.afterPropertiesSet();
                return executorService.getThreadPoolExecutor();
            }

            @Override
            public ExecutorService defaultCreate(String prefix, int coreSize, int maxThread, int queueSize) {
                return create(prefix, maxThread, coreSize, queueSize, new ThreadPoolExecutor.CallerRunsPolicy());
            }
        };
    }

}
