package com.twilight.aggregator.process.perp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.PanelMetrics;
import com.twilight.aggregator.model.perp.PerpSignal;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 趋势信号检测器 - 基于Panel Metrics生成拥挤度/清算风险信号
 * 
 * 检测规则（固定阈值 - GPT建议）：
 * 
 * 1. 拥挤度警告（CROWDING）：
 *    - crowding_score > 2.5 AND liquidity_regime == "THIN"
 *    - Level: CRITICAL
 * 
 * 2. 资金费率极端（CROWDING）：
 *    - abs(funding_rate_8h) > 0.01 (1%)
 *    - Level: WARNING
 * 
 * 3. OI快速增长+薄流动性（LIQUIDATION_RISK）：
 *    - oi_delta_pct > 5% AND liquidity_regime == "THIN"
 *    - Level: WARNING
 * 
 * 4. 基差异常（CROWDING）：
 *    - abs(basis_bps) > 100 (1%)
 *    - Level: WARNING
 * 
 * 输出：
 * - PerpSignal（信号对象）
 * - 写入Kafka（perp.signals）和ClickHouse（perp_signals）
 */
public class TrendSignalDetector extends ProcessFunction<PanelMetrics, PerpSignal> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TrendSignalDetector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    // 阈值配置
    private static final double CROWDING_SCORE_THRESHOLD = 2.5;
    private static final double FUNDING_RATE_8H_THRESHOLD = 0.01;  // 1%
    private static final double OI_DELTA_PCT_THRESHOLD = 5.0;      // 5%
    private static final double BASIS_EXTREME_THRESHOLD = 100.0;   // 100 bps = 1%
    
    @Override
    public void processElement(
            PanelMetrics panel,
            ProcessFunction<PanelMetrics, PerpSignal>.Context ctx,
            Collector<PerpSignal> out) throws Exception {
        
        // 1. 拥挤度 + 薄流动性
        if (panel.getCrowdingScore() != null && 
            panel.getCrowdingScore() > CROWDING_SCORE_THRESHOLD &&
            "THIN".equals(panel.getLiquidityRegime())) {
            
            PerpSignal signal = createSignal(
                    panel, 
                    PerpSignal.SignalType.CROWDING, 
                    PerpSignal.SignalLevel.CRITICAL,
                    "crowded_thin_market",
                    panel.getCrowdingScore(),
                    CROWDING_SCORE_THRESHOLD
            );
            signal.setSignalDetail(String.format(
                    "High crowding score (%.2f) in thin liquidity market", 
                    panel.getCrowdingScore()));
            out.collect(signal);
        }
        
        // 2. 资金费率极端
        if (panel.getFundingEma24h() != null) {
            double fundingEma = Math.abs(panel.getFundingEma24h().doubleValue());
            if (fundingEma > FUNDING_RATE_8H_THRESHOLD) {
                PerpSignal signal = createSignal(
                        panel,
                        PerpSignal.SignalType.CROWDING,
                        PerpSignal.SignalLevel.WARNING,
                        "extreme_funding",
                        fundingEma,
                        FUNDING_RATE_8H_THRESHOLD
                );
                signal.setSignalDetail(String.format(
                        "Extreme funding EMA 24h: %.4f%% (threshold: %.2f%%)",
                        fundingEma * 100, FUNDING_RATE_8H_THRESHOLD * 100));
                out.collect(signal);
            }
        }
        
        // 3. OI快速增长 + 薄流动性
        if (panel.getOiDelta1m() != null && panel.getOiUsd() != null) {
            double oiDeltaPct = panel.getOiDelta1m().doubleValue() / 
                    panel.getOiUsd().doubleValue() * 100;
            
            if (oiDeltaPct > OI_DELTA_PCT_THRESHOLD && 
                "THIN".equals(panel.getLiquidityRegime())) {
                PerpSignal signal = createSignal(
                        panel,
                        PerpSignal.SignalType.LIQUIDATION_RISK,
                        PerpSignal.SignalLevel.WARNING,
                        "oi_surge_thin",
                        oiDeltaPct,
                        OI_DELTA_PCT_THRESHOLD
                );
                signal.setSignalDetail(String.format(
                        "OI surged %.2f%% in thin liquidity (delta: %s USD)",
                        oiDeltaPct, panel.getOiDelta1m()));
                out.collect(signal);
            }
        }
        
        // 4. 基差异常
        if (panel.getBasisBps() != null) {
            double basisAbs = Math.abs(panel.getBasisBps());
            if (basisAbs > BASIS_EXTREME_THRESHOLD) {
                PerpSignal signal = createSignal(
                        panel,
                        PerpSignal.SignalType.CROWDING,
                        PerpSignal.SignalLevel.WARNING,
                        "extreme_basis",
                        basisAbs,
                        BASIS_EXTREME_THRESHOLD
                );
                signal.setSignalDetail(String.format(
                        "Extreme basis: %.2f bps (%.2f%%)",
                        panel.getBasisBps(), panel.getBasisBps() / 100));
                out.collect(signal);
            }
        }
    }
    
    /**
     * 创建信号对象
     */
    private PerpSignal createSignal(
            PanelMetrics panel,
            PerpSignal.SignalType signalType,
            PerpSignal.SignalLevel signalLevel,
            String metricName,
            double metricValue,
            double threshold) {
        
        PerpSignal signal = new PerpSignal();
        signal.setSymbol(panel.getSymbol());
        signal.setExchange(panel.getExchange());
        signal.setSignalTime(panel.getEndTime());
        signal.setSignalType(signalType);
        signal.setSignalLevel(signalLevel);
        signal.setMetricName(metricName);
        signal.setMetricValue(metricValue);
        signal.setThreshold(threshold);
        
        // 构建上下文JSON
        try {
            ObjectNode context = MAPPER.createObjectNode();
            context.put("liquidity_regime", panel.getLiquidityRegime());
            context.put("crowding_score", panel.getCrowdingScore());
            context.put("avg_spread_bps", panel.getAvgSpreadBps());
            context.put("avg_depth_50k", panel.getAvgDepth50k() != null ? 
                    panel.getAvgDepth50k().doubleValue() : 0);
            context.put("basis_bps", panel.getBasisBps());
            context.put("funding_ema_24h", panel.getFundingEma24h() != null ? 
                    panel.getFundingEma24h().doubleValue() : 0);
            context.put("oi_delta_1m", panel.getOiDelta1m() != null ? 
                    panel.getOiDelta1m().doubleValue() : 0);
            
            signal.setContextJson(context.toString());
        } catch (Exception e) {
            LOG.error("Failed to build signal context JSON", e);
            signal.setContextJson("{}");
        }
        
        return signal;
    }
}

