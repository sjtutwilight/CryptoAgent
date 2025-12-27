package com.twilight.backend.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * 时间范围工具类
 * 用于解析和处理时间范围参数
 */
@Component
public class TimeRangeUtil {

    /**
     * 时间范围数据类
     */
    @Data
    @AllArgsConstructor
    public static class TimeRange {
        private LocalDateTime startTime;
        private LocalDateTime endTime;

        /**
         * 获取时间跨度（分钟）
         */
        public long getDurationMinutes() {
            return ChronoUnit.MINUTES.between(startTime, endTime);
        }

        /**
         * 获取时间跨度（小时）
         */
        public long getDurationHours() {
            return ChronoUnit.HOURS.between(startTime, endTime);
        }

        /**
         * 获取时间跨度（天）
         */
        public long getDurationDays() {
            return ChronoUnit.DAYS.between(startTime, endTime);
        }
    }

    /**
     * 解析时间范围字符串
     * 
     * @param timeRange 时间范围字符串，如: 1h, 24h, 7d, 30d
     * @return 时间范围对象
     */
    public TimeRange parseTimeRange(String timeRange) {
        if (timeRange == null || timeRange.trim().isEmpty()) {
            timeRange = "24h"; // 默认24小时
        }

        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime;

        String range = timeRange.toLowerCase().trim();
        
        try {
            if (range.endsWith("m")) {
                // 分钟
                int minutes = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusMinutes(minutes);
            } else if (range.endsWith("h")) {
                // 小时
                int hours = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusHours(hours);
            } else if (range.endsWith("d")) {
                // 天
                int days = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusDays(days);
            } else if (range.endsWith("w")) {
                // 周
                int weeks = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusWeeks(weeks);
            } else {
                // 尝试解析为纯数字，默认为小时
                int hours = Integer.parseInt(range);
                startTime = endTime.minusHours(hours);
            }
        } catch (NumberFormatException e) {
            // 解析失败，使用默认24小时
            startTime = endTime.minusHours(24);
        }

        return new TimeRange(startTime, endTime);
    }

    /**
     * 解析时间范围字符串（指定结束时间）
     * 
     * @param timeRange 时间范围字符串
     * @param endTime 结束时间
     * @return 时间范围对象
     */
    public TimeRange parseTimeRange(String timeRange, LocalDateTime endTime) {
        if (timeRange == null || timeRange.trim().isEmpty()) {
            timeRange = "24h";
        }

        if (endTime == null) {
            endTime = LocalDateTime.now();
        }

        LocalDateTime startTime;
        String range = timeRange.toLowerCase().trim();
        
        try {
            if (range.endsWith("m")) {
                int minutes = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusMinutes(minutes);
            } else if (range.endsWith("h")) {
                int hours = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusHours(hours);
            } else if (range.endsWith("d")) {
                int days = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusDays(days);
            } else if (range.endsWith("w")) {
                int weeks = Integer.parseInt(range.substring(0, range.length() - 1));
                startTime = endTime.minusWeeks(weeks);
            } else {
                int hours = Integer.parseInt(range);
                startTime = endTime.minusHours(hours);
            }
        } catch (NumberFormatException e) {
            startTime = endTime.minusHours(24);
        }

        return new TimeRange(startTime, endTime);
    }

    /**
     * 创建最近N小时的时间范围
     * 
     * @param hours 小时数
     * @return 时间范围对象
     */
    public TimeRange createHoursRange(int hours) {
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusHours(hours);
        return new TimeRange(startTime, endTime);
    }

    /**
     * 创建最近N天的时间范围
     * 
     * @param days 天数
     * @return 时间范围对象
     */
    public TimeRange createDaysRange(int days) {
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusDays(days);
        return new TimeRange(startTime, endTime);
    }

    /**
     * 验证时间范围是否有效
     * 
     * @param timeRange 时间范围对象
     * @return 是否有效
     */
    public boolean isValidTimeRange(TimeRange timeRange) {
        if (timeRange == null || timeRange.getStartTime() == null || timeRange.getEndTime() == null) {
            return false;
        }
        return timeRange.getStartTime().isBefore(timeRange.getEndTime());
    }

    /**
     * 获取合适的数据聚合粒度
     * 根据时间跨度返回建议的聚合窗口
     * 
     * @param timeRange 时间范围
     * @return 聚合窗口 (1min, 5min, 1h, 1d)
     */
    public String getSuggestedAggregationWindow(TimeRange timeRange) {
        long hours = timeRange.getDurationHours();
        
        if (hours <= 6) {
            return "1min";
        } else if (hours <= 24) {
            return "5min";
        } else if (hours <= 168) { // 7天
            return "1h";
        } else {
            return "1d";
        }
    }
}



