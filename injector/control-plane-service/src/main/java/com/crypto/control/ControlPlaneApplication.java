package com.crypto.control;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * 控制平面服务主启动类
 */
@SpringBootApplication
@EnableScheduling  // 启用定时任务
@EnableKafka      // 启用Kafka
public class ControlPlaneApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(ControlPlaneApplication.class, args);
    }
}