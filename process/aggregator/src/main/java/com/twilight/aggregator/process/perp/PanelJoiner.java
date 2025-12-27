package com.twilight.aggregator.process.perp;

import com.twilight.aggregator.model.perp.ExecutionMetrics;
import com.twilight.aggregator.model.perp.ContextMetrics;
import com.twilight.aggregator.model.perp.PanelMetrics;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Panel Joiner - 连接执行面（1min rollup）和语境面（1min）
 * 
 * 架构：
 * - 使用CoProcessFunction维护两侧流的最新状态
 * - 当两侧都有数据时，输出PanelMetrics
 * - 允许一定容忍：语境面慢流可能延迟，执行面等待最多5秒
 * 
 * 水印对齐：
 * - 两侧都是1分钟粒度，end_time对齐到分钟边界
 * - 执行面：toMinute(end_time)
 * - 语境面：toMinute(end_time)
 * - Join条件：exec.end_time == ctx.end_time && exec.symbol == ctx.symbol
 * 
 * 输出：
 * - PanelMetrics（汇合后的完整面板）
 * - 后续流：LiquidityRegimeClassifier → CrowdingScoreCalculator → TrendSignalDetector
 */
public class PanelJoiner extends CoProcessFunction<ExecutionMetrics, ContextMetrics, PanelMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(PanelJoiner.class);
    
    // 状态：最新的执行面和语境面数据
    private transient ValueState<ExecutionMetrics> latestExecState;
    private transient ValueState<ContextMetrics> latestCtxState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        latestExecState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("latest-exec", ExecutionMetrics.class));
        latestCtxState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("latest-ctx", ContextMetrics.class));
    }
    
    /**
     * 处理执行面流（1min rollup后的ExecutionMetrics）
     */
    @Override
    public void processElement1(
            ExecutionMetrics exec,
            Context ctx,
            Collector<PanelMetrics> out) throws Exception {
        
        // 更新执行面状态
        latestExecState.update(exec);
        
        // 尝试join：如果语境面有数据且时间匹配
        ContextMetrics ctxMetrics = latestCtxState.value();
        if (ctxMetrics != null && canJoin(exec, ctxMetrics)) {
            PanelMetrics panel = buildPanel(exec, ctxMetrics);
            out.collect(panel);
            
            LOG.debug("Panel joined: {}@{} at {}", 
                    exec.getSymbol(), exec.getExchange(), exec.getEndTime());
            
            // 清理已join的状态
            latestExecState.clear();
            latestCtxState.clear();
        } else {
            LOG.debug("Exec received, waiting for Context: {}@{} at {}", 
                    exec.getSymbol(), exec.getExchange(), exec.getEndTime());
        }
    }
    
    /**
     * 处理语境面流（1min ContextMetrics）
     */
    @Override
    public void processElement2(
            ContextMetrics ctxMetrics,
            Context ctx,
            Collector<PanelMetrics> out) throws Exception {
        
        // 更新语境面状态
        latestCtxState.update(ctxMetrics);
        
        // 尝试join：如果执行面有数据且时间匹配
        ExecutionMetrics exec = latestExecState.value();
        if (exec != null && canJoin(exec, ctxMetrics)) {
            PanelMetrics panel = buildPanel(exec, ctxMetrics);
            out.collect(panel);
            
            LOG.debug("Panel joined: {}@{} at {}", 
                    exec.getSymbol(), exec.getExchange(), exec.getEndTime());
            
            // 清理已join的状态
            latestExecState.clear();
            latestCtxState.clear();
        } else {
            LOG.debug("Context received, waiting for Exec: {}@{} at {}", 
                    ctxMetrics.getSymbol(), ctxMetrics.getExchange(), ctxMetrics.getEndTime());
        }
    }
    
    /**
     * 判断能否join（时间窗口对齐）
     */
    private boolean canJoin(ExecutionMetrics exec, ContextMetrics ctx) {
        // 必须是同一个symbol和exchange
        if (!exec.getSymbol().equals(ctx.getSymbol()) || 
            !exec.getExchange().equals(ctx.getExchange())) {
            return false;
        }
        
        // 时间必须对齐到同一分钟
        // 两侧都是分钟边界，应该完全相等
        return exec.getEndTime().equals(ctx.getEndTime());
    }
    
    /**
     * 构建PanelMetrics（合并执行面和语境面）
     */
    private PanelMetrics buildPanel(ExecutionMetrics exec, ContextMetrics ctx) {
        PanelMetrics panel = new PanelMetrics();
        
        // 基础标识
        panel.setSymbol(exec.getSymbol());
        panel.setExchange(exec.getExchange());
        panel.setEndTime(exec.getEndTime());
        
        // 执行面聚合指标（从1s rollup）
        panel.setAvgSpreadBps(exec.getSpreadBps());           // avg
        panel.setMaxSpreadBps(exec.getSpreadAbs() != null ? 
                exec.getSpreadAbs().doubleValue() : null);   // max (复用字段)
        panel.setAvgDepth50k(exec.getDepth50k());             // avg
        panel.setAvgImpact50kBps(exec.getImpact50kBps());     // avg
        panel.setAvgImbalance(exec.getImbalanceTotal());      // avg
        panel.setSumOfi(exec.getOfi());                        // sum
        panel.setVolumeUsd(exec.getVolumeUsd());               // sum
        panel.setTradeCount(exec.getTradeCount());             // sum
        
        // 语境面指标（直接join）
        panel.setMarkPrice(ctx.getMarkPrice());
        panel.setBasisBps(ctx.getBasisBps());
        panel.setFundingRate(ctx.getFundingRate());
        panel.setFundingEma24h(ctx.getFundingEma24h());
        panel.setOiUsd(ctx.getOiUsd());
        panel.setOiDelta1m(ctx.getOiDelta1m());
        
        // 衍生指标初始为null，后续处理器填充
        panel.setLiquidityRegime(null);  // 由LiquidityRegimeClassifier填充
        panel.setCrowdingScore(null);     // 由CrowdingScoreCalculator填充
        
        return panel;
    }
}






