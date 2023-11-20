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

import static org.SyncDataCtlAutoConfiguration.DEFAULT_SCAN_MAPPER;
import static org.SyncDataCtlAutoConfiguration.INTERNAL_BASE_PACKAGE_JEXL;

/**
 * Description: 脚手架的自动装配
 * Author: 李开广
 * Date: 2023/5/24 2:38 PM
 */
@Configuration
@ConditionalOnWebApplication
@ConditionalOnClass(TaskDynamicConfig.class)
@ComponentScan(basePackages = {INTERNAL_BASE_PACKAGE_JEXL})
@MapperScan(basePackages = {DEFAULT_SCAN_MAPPER})
// aim to support AopContext#currentProxy()
@EnableAspectJAutoProxy(exposeProxy = true)
//@AutoConfigureBefore(MybatisPlusAutoConfiguration.class)
@Slf4j
public class SyncDataCtlAutoConfiguration implements SmartInitializingSingleton {

    protected static final String CUSTOM_BASE_PACKAGES = "${kg.job.base-packages:}'}";
    protected static final String INTERNAL_BASE_PACKAGE_JEXL ="${kg.job.internal-base-package:'org.kg," + CUSTOM_BASE_PACKAGES;

    protected static final String CUSTOM_SCAN_MAPPERS = "${kg.job.scan-mapper-packages:}'}";
    protected static final String DEFAULT_SCAN_MAPPER = "${kg.job.default-scan-mapper-packages:'com.**..mapper, com.**..dao, org.**..mapper, org.**..dao," + CUSTOM_SCAN_MAPPERS;


    @Value(INTERNAL_BASE_PACKAGE_JEXL)
    private String internalBasePackage;

    @Value(DEFAULT_SCAN_MAPPER)
    private String defaultScanMapper;

    @Value(CUSTOM_BASE_PACKAGES)
    private String basePackages;

    @Override
    public void afterSingletonsInstantiated() {
        log.info("------------- ctl load start ------------- >>>");
        log.info("clt default-packages={} default-scan-mapper={}", internalBasePackage, defaultScanMapper);
        log.info("ctl current job config:{}", TaskDynamicConfig.getAll());
        log.info("------------- ctl load end ------------- >>>");

    }

}
