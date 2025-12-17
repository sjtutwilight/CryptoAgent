package com.twilight.metadatacore;

import com.twilight.metadatacore.config.MetadataProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;

@EnableCaching
@SpringBootApplication
@EnableConfigurationProperties(MetadataProperties.class)
public class MetadataCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetadataCoreApplication.class, args);
    }
}
