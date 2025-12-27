package com.twilight.aggregator.process.perp;

import com.tdunning.math.stats.TDigest;
import com.twilight.aggregator.model.perp.PanelMetrics;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * 拥挤度得分计算器 - 使用T-Digest算法计算Z-score
 * 
 * 拥挤度得分公式（GPT建议）：
 * crowding_score = z(funding) * 0.4 + z(|basis|) * 0.3 + z(oi_delta) * 0.3
 * 
 * Z-score计算：
 * z(x) = (x - mean) / stddev
 * 
 * T-Digest方法：
 * - 维护24小时滚动窗口的统计摘要（轻量级，不存原始数据）
 * - 每个指标维护独立的TDigest实例
 * - TDigest提供mean、variance、stddev估算
 * - 内存友好：压缩率100，每个TDigest约4KB
 * 
 * 状态管理：
 * - MapState<Long, TDigestSnapshot> 存储 {timestamp -> 3个TDigest}
 * - 保留最近24小时数据（1440分钟）
 * - 定期清理过期数据（每小时触发一次）
 */
public class CrowdingScoreCalculator extends KeyedProcessFunction<String, PanelMetrics, PanelMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CrowdingScoreCalculator.class);
    
    // 配置常量
    private static final long WINDOW_SIZE_MS = 24 * 60 * 60 * 1000L;  // 24小时
    private static final int DIGEST_COMPRESSION = 100;  // TDigest压缩率
    private static final long CLEANUP_INTERVAL_MS = 60 * 60 * 1000L;  // 1小时清理一次
    
    // 权重配置（GPT建议）
    private static final double FUNDING_WEIGHT = 0.4;
    private static final double BASIS_WEIGHT = 0.3;
    private static final double OI_DELTA_WEIGHT = 0.3;
    
    // 状态：存储24小时历史数据的TDigest
    private transient MapState<Long, DigestTripleSnapshot> historyState;
    
    // 内存中的统计摘要（用于快速计算）
    private transient TDigest fundingDigest;
    private transient TDigest basisDigest;
    private transient TDigest oiDeltaDigest;
    
    // 上次清理时间
    private transient long lastCleanupTime;
    
    // 标志：TDigest是否已从state恢复
    private transient boolean digestsInitialized = false;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        historyState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("crowding-history", Long.class, DigestTripleSnapshot.class));
        
        // 初始化TDigest实例
        fundingDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        basisDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        oiDeltaDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        
        // 注意：不能在open()中访问MapState，因为还没有设置key
        // TDigest将在第一次processElement时从state恢复
        
        lastCleanupTime = System.currentTimeMillis();
    }
    
    @Override
    public void processElement(
            PanelMetrics panel,
            Context ctx,
            Collector<PanelMetrics> out) throws Exception {
        
        // 延迟初始化：第一次processElement时从state恢复TDigest
        if (!digestsInitialized) {
            rebuildDigestsFromState();
            digestsInitialized = true;
        }
        
        long currentTime = panel.getEndTime();
        
        // 提取指标值
        Double fundingRate = panel.getFundingRate() != null ? 
                panel.getFundingRate().doubleValue() : null;
        Double basisBps = panel.getBasisBps();
        Double oiDelta1m = panel.getOiDelta1m() != null ? 
                panel.getOiDelta1m().doubleValue() : null;
        
        // 更新TDigest（添加新数据点）
        if (fundingRate != null) {
            fundingDigest.add(fundingRate);
        }
        if (basisBps != null) {
            basisDigest.add(Math.abs(basisBps));  // 使用绝对值
        }
        if (oiDelta1m != null) {
            oiDeltaDigest.add(oiDelta1m);
        }
        
        // 保存当前快照到状态
        DigestTripleSnapshot snapshot = new DigestTripleSnapshot(
                fundingRate, Math.abs(basisBps != null ? basisBps : 0), oiDelta1m);
        historyState.put(currentTime, snapshot);
        
        // 计算Z-scores（如果有足够数据）
        if (fundingDigest.size() >= 10) {  // 至少10个数据点才计算
            Double zFunding = calculateZScore(fundingRate, fundingDigest);
            Double zBasis = calculateZScore(basisBps != null ? Math.abs(basisBps) : null, basisDigest);
            Double zOiDelta = calculateZScore(oiDelta1m, oiDeltaDigest);
            
            // 计算拥挤度得分
            if (zFunding != null && zBasis != null && zOiDelta != null) {
                double crowdingScore = 
                        zFunding * FUNDING_WEIGHT + 
                        zBasis * BASIS_WEIGHT + 
                        zOiDelta * OI_DELTA_WEIGHT;
                panel.setCrowdingScore(crowdingScore);
                
                LOG.debug("Crowding score calculated: {}@{} → {} (z_funding={}, z_basis={}, z_oi={})",
                        panel.getSymbol(), panel.getExchange(), crowdingScore, zFunding, zBasis, zOiDelta);
            } else {
                panel.setCrowdingScore(null);
                LOG.debug("Crowding score skipped (missing values): {}@{}", 
                        panel.getSymbol(), panel.getExchange());
            }
        } else {
            panel.setCrowdingScore(null);
            LOG.debug("Crowding score skipped (insufficient data): {}@{}, count={}", 
                    panel.getSymbol(), panel.getExchange(), fundingDigest.size());
        }
        
        // 定期清理过期数据
        if (currentTime - lastCleanupTime > CLEANUP_INTERVAL_MS) {
            cleanupExpiredData(currentTime);
            lastCleanupTime = currentTime;
        }
        
        out.collect(panel);
    }
    
    /**
     * 计算Z-score：z = (x - mean) / stddev
     */
    private Double calculateZScore(Double value, TDigest digest) {
        if (value == null || digest.size() == 0) {
            return null;
        }
        
        // TDigest不直接提供mean/stddev，需要通过quantile估算
        // 简化方案：使用中位数作为mean，IQR估算stddev
        double median = digest.quantile(0.5);
        double q25 = digest.quantile(0.25);
        double q75 = digest.quantile(0.75);
        double iqr = q75 - q25;
        double stddev = iqr / 1.349;  // IQR转stddev的近似公式
        
        if (stddev < 1e-9) {  // 避免除零
            return 0.0;
        }
        
        return (value - median) / stddev;
    }
    
    /**
     * 清理超过24小时的历史数据
     */
    private void cleanupExpiredData(long currentTime) throws Exception {
        long expiryTime = currentTime - WINDOW_SIZE_MS;
        
        int removedCount = 0;
        for (Long timestamp : historyState.keys()) {
            if (timestamp < expiryTime) {
                historyState.remove(timestamp);
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            // 重建TDigest（从剩余状态）
            rebuildDigestsFromState();
            LOG.info("Cleaned up {} expired crowding history entries, rebuilt TDigest", removedCount);
        }
    }
    
    /**
     * 从状态重建TDigest（用于恢复或清理后）
     */
    private void rebuildDigestsFromState() throws Exception {
        fundingDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        basisDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        oiDeltaDigest = TDigest.createDigest(DIGEST_COMPRESSION);
        
        for (DigestTripleSnapshot snapshot : historyState.values()) {
            if (snapshot.fundingRate != null) {
                fundingDigest.add(snapshot.fundingRate);
            }
            if (snapshot.basisAbs != null) {
                basisDigest.add(snapshot.basisAbs);
            }
            if (snapshot.oiDelta != null) {
                oiDeltaDigest.add(snapshot.oiDelta);
            }
        }
        
        LOG.debug("Rebuilt TDigest from state: funding_size={}, basis_size={}, oi_size={}",
                fundingDigest.size(), basisDigest.size(), oiDeltaDigest.size());
    }
    
    /**
     * 内部类：存储单个时间点的三个指标快照
     */
    public static class DigestTripleSnapshot implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public Double fundingRate;
        public Double basisAbs;
        public Double oiDelta;
        
        public DigestTripleSnapshot() {}
        
        public DigestTripleSnapshot(Double fundingRate, Double basisAbs, Double oiDelta) {
            this.fundingRate = fundingRate;
            this.basisAbs = basisAbs;
            this.oiDelta = oiDelta;
        }
    }
}

