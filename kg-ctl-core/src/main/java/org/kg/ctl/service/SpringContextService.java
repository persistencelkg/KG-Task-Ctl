package org.kg.ctl.service;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * @author likaiguang
 * @date 2023/4/12 10:36 上午
 */
@Component
public class SpringContextService implements ApplicationContextAware {

    private static ApplicationContext applicationContext;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (Objects.nonNull(SpringContextService.applicationContext)) {
            return;
        }
        SpringContextService.applicationContext = applicationContext;
    }

    public static String getApplicationName() {
        return applicationContext.getApplicationName() ;
    }

    public static <T> T getBean(Class<T> beanClass) {

        return applicationContext.getBean(beanClass);
    }


}
