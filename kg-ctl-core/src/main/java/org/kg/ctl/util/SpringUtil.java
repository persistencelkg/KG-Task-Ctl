package org.kg.ctl.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Description:
 * Author: 李开广
 * Date: 2023/6/1 8:43 PM
 */
@Component
public class SpringUtil implements ApplicationContextAware {

    private static ApplicationContext APPLICATION_CONTEXT;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (Objects.isNull(APPLICATION_CONTEXT)) {
            APPLICATION_CONTEXT = applicationContext;
        }
    }

    public static void publishEvent(ApplicationEvent applicationEvent) {
        APPLICATION_CONTEXT.publishEvent(applicationEvent);
    }

    public static <T> T getBean(String name, Class<T> type) {
        return APPLICATION_CONTEXT.getBean(name, type);
    }
}
