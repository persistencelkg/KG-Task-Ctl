package org;

import lombok.extern.slf4j.Slf4j;
import org.kg.ctl.dao.TaskDynamicConfig;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import javax.annotation.PostConstruct;

/**
 * @description: 脚手架的自动装配
 * @author: 李开广
 * @date: 2023/5/24 2:38 PM
 */
@Configuration
@ConditionalOnWebApplication
@ConditionalOnClass(TaskDynamicConfig.class)
@ComponentScan(basePackages = {"${kg.job.internal-base-package}", "${kg.job.base-packages}"})
@MapperScan(value = {"${kg.job.default-scan-mapper-packages}"})
// aim to support AopContext#currentProxy()
@EnableAspectJAutoProxy(exposeProxy = true)
//@AutoConfigureBefore(MybatisPlusAutoConfiguration.class)
@Slf4j
public class SyncDataCtlAutoConfiguration implements SmartInitializingSingleton {

    @Value("${kg.job.internal-base-package}")
    private String internalBasePackage;

    @Value("${kg.job.default-scan-mapper-packages}")
    private String defaultScanMapper;

    @Value("${kg.job.base-packages:}")
    private String basePackages;

    @Override
    public void afterSingletonsInstantiated() {
        log.info("------------- ctl load start ------------- >>>");
        log.info("clt default-packages={} default-scan-mapper={}", String.join(",", internalBasePackage, basePackages), defaultScanMapper);
        log.info("ctl current job config:{}", TaskDynamicConfig.getAll());
        log.info("------------- ctl load end ------------- >>>");
    }
}
