package com.twilight.aggregator.process.perp;

import com.twilight.aggregator.model.perp.PanelMetrics;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 流动性制度分类器 - 基于点差和深度判定流动性水平
 * 
 * 分类规则（使用固定阈值）：
 * 
 * THICK（厚流动性）:
 *   - avg_spread_bps < 3 (点差小) AND avg_depth_50k > 100000 (深度大)
 * 
 * THIN（薄流动性）:
 *   - avg_spread_bps > 10 (点差大) OR avg_depth_50k < 30000 (深度小)
 * 
 * NORMAL（正常流动性）:
 *   - 其他情况
 * 
 * 注：GPT建议初版使用固定阈值，后续可迭代为动态分位数（从Redis读取p25/p75）
 */
public class LiquidityRegimeClassifier extends ProcessFunction<PanelMetrics, PanelMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(LiquidityRegimeClassifier.class);
    
    // 固定阈值配置
    private static final double THICK_SPREAD_THRESHOLD = 3.0;      // < 3 bps为窄点差
    private static final double THICK_DEPTH_THRESHOLD = 100000.0;  // > 100k USD为深度厚
    private static final double THIN_SPREAD_THRESHOLD = 10.0;      // > 10 bps为宽点差
    private static final double THIN_DEPTH_THRESHOLD = 30000.0;    // < 30k USD为深度薄
    
    @Override
    public void processElement(
            PanelMetrics panel,
            ProcessFunction<PanelMetrics, PanelMetrics>.Context ctx,
            Collector<PanelMetrics> out) throws Exception {
        
        // 获取点差和深度
        Double avgSpreadBps = panel.getAvgSpreadBps();
        Double avgDepth50k = panel.getAvgDepth50k() != null ? 
                panel.getAvgDepth50k().doubleValue() : null;
        
        // 处理缺失值
        if (avgSpreadBps == null || avgDepth50k == null) {
            panel.setLiquidityRegime("UNKNOWN");
            out.collect(panel);
            LOG.warn("Missing data for liquidity classification: {}@{}, spread={}, depth={}", 
                    panel.getSymbol(), panel.getExchange(), avgSpreadBps, avgDepth50k);
            return;
        }
        
        // 分类逻辑
        String regime;
        
        if (avgSpreadBps < THICK_SPREAD_THRESHOLD && avgDepth50k > THICK_DEPTH_THRESHOLD) {
            // 窄点差 + 深度厚 = THICK
            regime = "THICK";
        } else if (avgSpreadBps > THIN_SPREAD_THRESHOLD || avgDepth50k < THIN_DEPTH_THRESHOLD) {
            // 宽点差 或 深度薄 = THIN
            regime = "THIN";
        } else {
            // 其他 = NORMAL
            regime = "NORMAL";
        }
        
        panel.setLiquidityRegime(regime);
        out.collect(panel);
        
        LOG.debug("Liquidity regime classified: {}@{} → {} (spread={}, depth={})",
                panel.getSymbol(), panel.getExchange(), regime, avgSpreadBps, avgDepth50k);
    }
}

