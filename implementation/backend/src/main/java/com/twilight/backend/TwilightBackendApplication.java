package com.twilight.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Twilight数据平台Web后端主启动类
 */
@SpringBootApplication
@EnableScheduling
public class TwilightBackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(TwilightBackendApplication.class, args);
    }
}
