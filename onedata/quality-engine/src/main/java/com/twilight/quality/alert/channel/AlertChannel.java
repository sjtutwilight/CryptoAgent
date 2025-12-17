package com.twilight.quality.alert.channel;

import com.twilight.quality.domain.alert.QualityAlert;
import com.twilight.quality.domain.enums.AlertLevel;

/**
 * 告警通道接口
 */
public interface AlertChannel {
    
    /**
     * 获取通道名称
     */
    String getName();
    
    /**
     * 通道是否启用
     */
    boolean isEnabled();
    
    /**
     * 获取最低告警级别
     */
    AlertLevel getMinLevel();
    
    /**
     * 发送告警
     */
    void send(QualityAlert alert);
}

