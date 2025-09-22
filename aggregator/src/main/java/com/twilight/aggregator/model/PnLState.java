package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * PnL状态存储
 * 极小状态设计，每个(accountId, tokenId)维护一份状态
 * 基于移动加权平均成本(MA)算法
 */
@Data
@NoArgsConstructor
public class PnLState implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(PnLState.class);
    
    // 精度配置：使用18位小数精度，与以太坊wei精度保持一致
    public static final int SCALE = 18;
    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;
    
    // 核心状态字段 - 极小设计
    private BigDecimal position = BigDecimal.ZERO;         // pos: 当前持仓数量
    private BigDecimal avgCost = BigDecimal.ZERO;          // avg: 移动加权平均成本价
    private BigDecimal realizedCost = BigDecimal.ZERO;     // rc: 已实现成本累计
    private BigDecimal realizedProceeds = BigDecimal.ZERO; // rp: 已实现收入累计
    private BigDecimal realizedPnL = BigDecimal.ZERO;      // rpn: 已实现盈亏累计
    private Long lastTxTime = 0L;                          // lastTx: 最后交易时间(毫秒)
    
    /**
     * 处理买入交易
     * 更新移动加权平均成本和持仓数量
     */
    public void processBuy(BigDecimal quantity, BigDecimal price, Long blockTimeMs) {
        if (quantity.compareTo(BigDecimal.ZERO) <= 0 || price.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("⚠️ Invalid buy trade data - quantity={}, price={}", quantity, price);
            return; // 忽略无效的交易数据
        }
        BigDecimal newPosition = position.add(quantity);
        
        // 移动加权平均成本 = (原持仓*原成本 + 新买入数量*新价格) / 新总持仓
        BigDecimal totalCost = position.multiply(avgCost).add(quantity.multiply(price));
        BigDecimal newAvgCost = newPosition.compareTo(BigDecimal.ZERO) > 0 ? 
            totalCost.divide(newPosition, SCALE, ROUNDING_MODE) : BigDecimal.ZERO;
        
        this.position = newPosition;
        this.avgCost = newAvgCost;
        this.lastTxTime = Math.max(this.lastTxTime, blockTimeMs);
    }
    
    /**
     * 处理卖出交易的返回结果
     */
    @Data
    @AllArgsConstructor
    public static class SellResult {
        private final BigDecimal realizedQty;        // 实际卖出数量
        private final BigDecimal realizedCostUsd;    // 已实现成本
        private final BigDecimal realizedProceedsUsd; // 已实现收入
        private final BigDecimal realizedPnLUsd;     // 已实现盈亏
        
        public boolean hasRealized() {
            return realizedQty.compareTo(BigDecimal.ZERO) > 0;
        }
    }

    /**
     * 处理卖出交易
     * 计算已实现盈亏，更新持仓数量
     * @return 已实现盈亏详情，如果没有实际卖出则返回null
     */
    public SellResult processSell(BigDecimal quantity, BigDecimal price, Long blockTimeMs) {
        if (quantity.compareTo(BigDecimal.ZERO) <= 0 || price.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("⚠️ Invalid sell trade data - quantity={}, price={}", quantity, price);
            return null; // 忽略无效的交易数据
        }
        // 严格模式：禁止超卖，实际卖出数量不能超过持仓
        BigDecimal actualSellQty = quantity.min(position);
        if (actualSellQty.compareTo(BigDecimal.ZERO) > 0) {
            // 计算已实现成本和收入
            BigDecimal sellCost = actualSellQty.multiply(avgCost);
            BigDecimal sellProceeds = actualSellQty.multiply(price);
            BigDecimal sellPnL = actualSellQty.multiply(price.subtract(avgCost));
            
            // 更新累计已实现数据
            this.realizedCost = realizedCost.add(sellCost);
            this.realizedProceeds = realizedProceeds.add(sellProceeds);
            this.realizedPnL = realizedPnL.add(sellPnL);
            
            // 对极小的值进行清理，避免浮点精度问题
            if (this.realizedPnL.abs().compareTo(new BigDecimal("0.0001")) < 0) {
                this.realizedPnL = BigDecimal.ZERO;
            }
            
            // 更新持仓
            this.position = position.subtract(actualSellQty);
            log.info("📤 sellCost={}, sellProceeds={}, realizedCost={}, realizedProceeds={}, realizedPnL={}", sellCost, sellProceeds, realizedCost, realizedProceeds, realizedPnL);
            // 如果持仓为0，重置平均成本
            if (this.position.compareTo(BigDecimal.ZERO) == 0) {
                this.avgCost = BigDecimal.ZERO;
            }
            
            this.lastTxTime = Math.max(this.lastTxTime, blockTimeMs);
            
            // 返回已实现盈亏详情
            return new SellResult(actualSellQty, sellCost, sellProceeds, sellPnL);
        }
        
        this.lastTxTime = Math.max(this.lastTxTime, blockTimeMs);
        return null; // 没有实际卖出
    }
    
    /**
     * 计算未实现盈亏
     */
    public BigDecimal calculateUnrealizedPnL(BigDecimal currentPrice) {
        if (position.compareTo(BigDecimal.ZERO) == 0 || currentPrice.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }
        return position.multiply(currentPrice.subtract(avgCost));
    }
    
    /**
     * 计算总盈亏
     */
    public BigDecimal calculateTotalPnL(BigDecimal currentPrice) {
        return realizedPnL.add(calculateUnrealizedPnL(currentPrice));
    }
    
    /**
     * 计算投资基数 (成本基础)
     */
    public BigDecimal calculateInvestmentBase() {
        return realizedCost.add(position.multiply(avgCost));
    }
    
    /**
     * 计算投资回报率 (ROI)
     */
    public double calculateROI(BigDecimal currentPrice) {
        BigDecimal investmentBase = calculateInvestmentBase();
        if (investmentBase.compareTo(BigDecimal.ZERO) == 0) {
            return 0.0;
        }
        
        BigDecimal totalPnL = calculateTotalPnL(currentPrice);
        return totalPnL.divide(investmentBase, 12, ROUNDING_MODE).doubleValue();
    }
    
    /**
     * 计算持仓占比
     */
    public double calculateHoldingPercentage() {
        BigDecimal investmentBase = calculateInvestmentBase();
        if (investmentBase.compareTo(BigDecimal.ZERO) == 0) {
            return 0.0;
        }
        
        BigDecimal currentHolding = position.multiply(avgCost);
        return currentHolding.divide(investmentBase, 12, ROUNDING_MODE).doubleValue();
    }
    
    /**
     * 检查状态是否有效
     */
    public boolean isValid() {
        return position.compareTo(BigDecimal.ZERO) >= 0 && 
               avgCost.compareTo(BigDecimal.ZERO) >= 0 &&
               lastTxTime > 0;
    }
    
    /**
     * 检查是否有持仓
     */
    public boolean hasPosition() {
        return position.compareTo(BigDecimal.ZERO) > 0;
    }
    
    /**
     * 获取状态摘要（用于日志）
     */
    public String getSummary() {
        return String.format("pos=%.4f, avg=%.4f, realizedPnL=%.2f, lastTx=%d", 
                           position.doubleValue(), avgCost.doubleValue(), 
                           realizedPnL.doubleValue(), lastTxTime);
    }
}
