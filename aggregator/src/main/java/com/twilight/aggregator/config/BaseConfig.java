package com.twilight.aggregator.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = LoggerFactory.getLogger(BaseConfig.class);
    protected final Properties properties;

    protected BaseConfig() {
        properties = new Properties();
        loadConfig();
    }

    protected void loadConfig() {

        // 首先尝试从系统属性获取环境
        String env = System.getProperty("env");
        if (env == null) {
            // 如果系统属性中没有，尝试从环境变量获取
            env = System.getenv("APP_ENV");
            if (env == null) {
                // 如果环境变量也没有，使用默认值
                env = "dev";
                System.out.println("Using default environment: dev");
            }
        }
        env = env.toLowerCase();

        String configFile = String.format("application-%s.properties", env);
        LOG.info("Loading configuration from: {}", configFile);

        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                LOG.error("Unable to find {}", configFile);
                throw new RuntimeException("Configuration file not found: " + configFile);
            }
            properties.load(input);
            System.out.println("Loaded properties: " + properties);
        } catch (IOException e) {
            LOG.error("Error loading configuration", e);
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    protected String getProperty(String key) {
        // 首先尝试从环境变量获取
        String envKey = key.toUpperCase().replace('.', '_');
        String value = System.getenv(envKey);
        System.out.println("envKey: " + envKey);
        System.out.println("value: " + value);
        if (value != null) {
            return value;
        }
        // 如果环境变量中没有，从配置文件获取
        String propertyValue = properties.getProperty(key);
        System.out.println("propertyValue: " + propertyValue);
        return propertyValue;
    }

    protected String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value != null ? value.trim() : defaultValue;
    }

    protected int getIntProperty(String key, String defaultValue) {
        return Integer.parseInt(getProperty(key, defaultValue));
    }

    protected long getLongProperty(String key, String defaultValue) {
        return Long.parseLong(getProperty(key, defaultValue));
    }

    protected boolean getBooleanProperty(String key, String defaultValue) {
        return Boolean.parseBoolean(getProperty(key, defaultValue));
    }
}