package com.twilight.aggregator.process.perp;

import com.twilight.aggregator.model.perp.ExecutionMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * ExecutionMetrics Rollup - 将1秒数据聚合到1分钟
 * 
 * 聚合逻辑：
 * - avg_spread_bps = avg(spread_bps)
 * - max_spread_bps = max(spread_bps)
 * - avg_depth_50k = avg(depth_50k)
 * - avg_impact_50k_bps = avg(impact_50k_bps)
 * - avg_imbalance = avg(imbalance_total)
 * - sum_ofi = sum(ofi)
 * - volume_usd = sum(volume_usd)
 * - trade_count = sum(trade_count)
 * 
 * 输入：1秒级ExecutionMetrics
 * 输出：ExecutionMetrics（1分钟级）- 复用同一数据模型，字段语义变为聚合值
 */
public class ExecutionMetricsRollup extends ProcessWindowFunction<ExecutionMetrics, ExecutionMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionMetricsRollup.class);
    
    @Override
    public void process(
            String key,
            Context context,
            Iterable<ExecutionMetrics> elements,
            Collector<ExecutionMetrics> out) throws Exception {
        
        // 聚合变量
        double sumSpreadBps = 0;
        double maxSpreadBps = Double.MIN_VALUE;
        BigDecimal sumDepth50k = BigDecimal.ZERO;
        double sumImpact50kBps = 0;
        double sumImbalance = 0;
        double sumOfi = 0;
        BigDecimal sumVolumeUsd = BigDecimal.ZERO;
        int sumTradeCount = 0;
        
        int count = 0;
        String symbol = null;
        String exchange = null;
        
        // 遍历1分钟内的所有1秒数据
        for (ExecutionMetrics metrics : elements) {
            if (symbol == null) {
                symbol = metrics.getSymbol();
                exchange = metrics.getExchange();
            }
            
            // 点差聚合
            if (metrics.getSpreadBps() != null) {
                sumSpreadBps += metrics.getSpreadBps();
                maxSpreadBps = Math.max(maxSpreadBps, metrics.getSpreadBps());
            }
            
            // 深度聚合
            if (metrics.getDepth50k() != null) {
                sumDepth50k = sumDepth50k.add(metrics.getDepth50k());
            }
            
            // 冲击成本聚合
            if (metrics.getImpact50kBps() != null) {
                sumImpact50kBps += metrics.getImpact50kBps();
            }
            
            // 不平衡聚合
            if (metrics.getImbalanceTotal() != null) {
                sumImbalance += metrics.getImbalanceTotal();
            }
            
            // OFI求和
            if (metrics.getOfi() != null) {
                sumOfi += metrics.getOfi();
            }
            
            // 成交量求和
            if (metrics.getVolumeUsd() != null) {
                sumVolumeUsd = sumVolumeUsd.add(metrics.getVolumeUsd());
            }
            
            // 成交笔数求和
            if (metrics.getTradeCount() != null) {
                sumTradeCount += metrics.getTradeCount();
            }
            
            count++;
        }
        
        // 处理空窗口（极少见）
        if (count == 0) {
            LOG.warn("Empty window for key: {}, window: [{}, {})", 
                    key, context.window().getStart(), context.window().getEnd());
            return;
        }
        
        // 构建1分钟级ExecutionMetrics
        ExecutionMetrics rollup = new ExecutionMetrics();
        rollup.setSymbol(symbol);
        rollup.setExchange(exchange);
        rollup.setEndTime(context.window().getEnd());  // 使用窗口结束时间
        
        // 设置聚合指标
        rollup.setSpreadBps(sumSpreadBps / count);  // avg_spread_bps
        rollup.setSpreadAbs(BigDecimal.valueOf(maxSpreadBps));  // 复用字段存max_spread_bps
        
        if (!sumDepth50k.equals(BigDecimal.ZERO)) {
            rollup.setDepth50k(sumDepth50k.divide(
                    BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP));  // avg_depth_50k
        }
        
        rollup.setImpact50kBps(sumImpact50kBps / count);  // avg_impact_50k_bps
        rollup.setImbalanceTotal(sumImbalance / count);   // avg_imbalance
        rollup.setOfi(sumOfi);                             // sum_ofi
        rollup.setVolumeUsd(sumVolumeUsd);                 // sum_volume_usd
        rollup.setTradeCount(sumTradeCount);               // sum_trade_count
        
        out.collect(rollup);
        
        LOG.debug("Rollup completed for {}@{}: {} records → 1min summary (avg_spread={}, max_spread={}, sum_ofi={}, volume={})",
                symbol, exchange, count, rollup.getSpreadBps(), maxSpreadBps, sumOfi, sumVolumeUsd);
    }
}



