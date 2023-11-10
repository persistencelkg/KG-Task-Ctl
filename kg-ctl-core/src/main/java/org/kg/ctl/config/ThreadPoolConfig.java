package org.kg.ctl.config;

import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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


    public static class MdcTaskDecorator implements TaskDecorator {

        @Override
        public Runnable decorate(Runnable runnable) {
            Map<String, String> copyOfContextMap = MDC.getCopyOfContextMap();
            return () -> {
                try {
                    MDC.setContextMap(copyOfContextMap);
                    runnable.run();
                } finally {
                    MDC.clear();
                }
            };
        }
    }


    interface CustomExecutorServiceFactory {
        /**
         *
         * @return @link CustomExecutorServiceFactory
         */
        ExecutorService create(String prefix, int maxThread, int coreSize, int queueSize, RejectedExecutionHandler rejectedExecutionHandler);

        /**
         * 创建默认调用者的线程
         * @param prefix
         * @param maxThread
         * @param coreSize
         * @param queueSize
         * @return
         */
        ExecutorService defaultCreate(String prefix, int maxThread, int coreSize, int queueSize);
    }

    /**
     * 多线程之间的传递问题
     * @return 被装饰的mdc
     */
    @Bean
    public MdcTaskDecorator mdcTaskDecorator() {
        return new MdcTaskDecorator();
    }

    @Bean
    public CustomExecutorServiceFactory selfExecutorService(MdcTaskDecorator mdcTaskDecorator) {
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
                executorService.setTaskDecorator(mdcTaskDecorator);
                return executorService.getThreadPoolExecutor();
            }

            @Override
            public ExecutorService defaultCreate(String prefix, int maxThread, int coreSize, int queueSize) {
                return create(prefix, maxThread, coreSize, queueSize, new ThreadPoolExecutor.CallerRunsPolicy());
            }
        };
    }

}
