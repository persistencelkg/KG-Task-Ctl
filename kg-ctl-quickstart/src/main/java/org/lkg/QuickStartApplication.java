package org.lkg;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/27 4:52 PM
 */
@SpringBootApplication
@MapperScan("org.lkg.mapper")
public class QuickStartApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(QuickStartApplication.class);
        springApplication.run(args);
    }
}
