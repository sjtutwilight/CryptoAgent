package com.twilight.backend.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * 时区转换工具类
 * 处理本地时间（CST/UTC+8）与ClickHouse时间（UTC）之间的转换
 */
public class TimeZoneUtil {
    
    private static final ZoneId LOCAL_ZONE = ZoneId.of("Asia/Shanghai"); // UTC+8
    private static final ZoneId CLICKHOUSE_ZONE = ZoneOffset.UTC;        // UTC
    
    /**
     * 将本地时间转换为ClickHouse UTC时间
     * 
     * @param localTime 本地时间
     * @return UTC时间字符串，格式: 'YYYY-MM-DD HH:mm:ss'
     */
    public static String toClickHouseTimeString(LocalDateTime localTime) {
        if (localTime == null) {
            return null;
        }
        
        // 将本地时间转换为UTC时间
        ZonedDateTime localZoned = localTime.atZone(LOCAL_ZONE);
        ZonedDateTime utcZoned = localZoned.withZoneSameInstant(CLICKHOUSE_ZONE);
        LocalDateTime utcTime = utcZoned.toLocalDateTime();
        
        // 格式化为ClickHouse可接受的字符串格式
        return utcTime.withNano(0).toString().replace("T", " ");
    }
    
    /**
     * 将ClickHouse UTC时间转换为本地时间
     * 
     * @param utcTime UTC时间
     * @return 本地时间
     */
    public static LocalDateTime fromClickHouseTime(LocalDateTime utcTime) {
        if (utcTime == null) {
            return null;
        }
        
        // 将UTC时间转换为本地时间
        ZonedDateTime utcZoned = utcTime.atZone(CLICKHOUSE_ZONE);
        ZonedDateTime localZoned = utcZoned.withZoneSameInstant(LOCAL_ZONE);
        return localZoned.toLocalDateTime();
    }
    
    /**
     * 获取当前UTC时间的字符串表示
     * 
     * @return 当前UTC时间字符串
     */
    public static String getCurrentClickHouseTimeString() {
        return toClickHouseTimeString(LocalDateTime.now());
    }
}
