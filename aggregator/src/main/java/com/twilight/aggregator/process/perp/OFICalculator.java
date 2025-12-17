package com.twilight.aggregator.process.perp;

import java.math.BigDecimal;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.perp.OrderBookSummary;

/**
 * OFI (Order Flow Imbalance) 计算器 - L1版本
 * 
 * 基于Kyle/Cont-Kukanov-Stoikov的标准OFI定义：
 * 
 * OFI_t = Δq_L1_bid · I{p_L1_bid 不降} - Δq_L1_ask · I{p_L1_ask 不升}
 * 
 * 其中：
 * - Δq_L1_bid: L1买单数量变化 = q_bid(t) - q_bid(t-1)
 * - Δq_L1_ask: L1卖单数量变化 = q_ask(t) - q_ask(t-1)
 * - I{p_L1_bid 不降}: 买价不降时为1，否则为0
 * - I{p_L1_ask 不升}: 卖价不升时为1，否则为0
 * 
 * 含义：
 * - 正值：买盘流入 > 卖盘流入，看多信号
 * - 负值：卖盘流入 > 买盘流入，看空信号
 * - 当价位变动时，跨档移动的量会被排除（通过指示函数）
 * 
 * 注意：这是标准的L1 OFI，不是价格加权版本
 */
public class OFICalculator extends KeyedProcessFunction<String, OrderBookSummary, OrderBookSummary> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(OFICalculator.class);
    
    // L1状态：记录上一次的L1价格和数量
    private transient ValueState<L1State> l1State;
    
    /**
     * L1状态：记录上一个窗口的L1档位
     */
    private static class L1State {
        BigDecimal prevBidPrice;
        BigDecimal prevBidSize;
        BigDecimal prevAskPrice;
        BigDecimal prevAskSize;
        
        L1State(BigDecimal prevBidPrice, BigDecimal prevBidSize, 
                BigDecimal prevAskPrice, BigDecimal prevAskSize) {
            this.prevBidPrice = prevBidPrice;
            this.prevBidSize = prevBidSize;
            this.prevAskPrice = prevAskPrice;
            this.prevAskSize = prevAskSize;
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化L1状态（2小时TTL）
        ValueStateDescriptor<L1State> descriptor = new ValueStateDescriptor<>(
            "ofi-l1-state",
            L1State.class
        );
        descriptor.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
            .newBuilder(Time.hours(2))
            .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        );
        l1State = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(OrderBookSummary summary, Context ctx, Collector<OrderBookSummary> out) 
            throws Exception {
        
        // 获取当前L1档位
        BigDecimal currBidPrice = summary.getBestBid();
        BigDecimal currBidSize = summary.getBestBidSize();
        BigDecimal currAskPrice = summary.getBestAsk();
        BigDecimal currAskSize = summary.getBestAskSize();
        
        // 获取前一次L1状态
        L1State prevState = l1State.value();
        
        double ofi = 0.0;
        
        if (prevState != null) {
            // 计算Δq（数量变化）
            double deltaBidSize = 0.0;
            double deltaAskSize = 0.0;
            
            if (currBidSize != null && prevState.prevBidSize != null) {
                deltaBidSize = currBidSize.subtract(prevState.prevBidSize).doubleValue();
            }
            
            if (currAskSize != null && prevState.prevAskSize != null) {
                deltaAskSize = currAskSize.subtract(prevState.prevAskSize).doubleValue();
            }
            
            // 计算指示函数
            // I{p_L1_bid 不降}: 当前买价 >= 前买价
            boolean bidPriceNotDown = false;
            if (currBidPrice != null && prevState.prevBidPrice != null) {
                bidPriceNotDown = currBidPrice.compareTo(prevState.prevBidPrice) >= 0;
            }
            
            // I{p_L1_ask 不升}: 当前卖价 <= 前卖价
            boolean askPriceNotUp = false;
            if (currAskPrice != null && prevState.prevAskPrice != null) {
                askPriceNotUp = currAskPrice.compareTo(prevState.prevAskPrice) <= 0;
            }
            
            // 计算OFI
            double bidComponent = bidPriceNotDown ? deltaBidSize : 0.0;
            double askComponent = askPriceNotUp ? deltaAskSize : 0.0;
            ofi = bidComponent - askComponent;
            
            if (log.isTraceEnabled()) {
                log.trace("OFI calculation: symbol={}, deltaBid={}, deltaAsk={}, " +
                         "bidPriceNotDown={}, askPriceNotUp={}, OFI={}", 
                         summary.getSymbol(), deltaBidSize, deltaAskSize, 
                         bidPriceNotDown, askPriceNotUp, ofi);
            }
        }
        
        // 更新状态
        l1State.update(new L1State(currBidPrice, currBidSize, currAskPrice, currAskSize));
        
        // 将OFI添加到summary中（注意：OrderBookSummary没有ofi字段，需要在ExecutionMetrics中添加）
        // 这里我们通过outputTag或直接传递到下游ExecutionMetricsBuilder
        // 暂时记录日志
        if (log.isDebugEnabled()) {
            log.debug("OFI: symbol={}, ofi={}, mid={}", 
                     summary.getSymbol(), ofi, summary.getMidPrice());
        }
        
        // 输出（OFI值将在ExecutionMetricsBuilder中使用）
        out.collect(summary);
    }
}







