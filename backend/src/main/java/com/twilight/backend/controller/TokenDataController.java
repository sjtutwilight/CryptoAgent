package com.twilight.backend.controller;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.TokenPriceHistory;
import com.twilight.backend.repository.AccountRepository;
import com.twilight.backend.repository.DistributionRepository;
import com.twilight.backend.repository.PnLRepository;
import com.twilight.backend.repository.TokenRepository;
import com.twilight.backend.repository.TradeRepository;
import com.twilight.backend.service.WebSocketService;
import com.twilight.backend.util.TimeRangeUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 代币数据接口 - 精简版
 * 专为前端展示设计，支持WebSocket实时推送
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = "代币数据接口", description = "代币相关数据的REST API和WebSocket推送")
public class TokenDataController {

    private final WebSocketService webSocketService;
    private final TokenRepository tokenRepository;
    private final TradeRepository tradeRepository;
    private final PnLRepository pnLRepository;
    private final DistributionRepository distributionRepository;
    private final AccountRepository accountRepository;
    private final TimeRangeUtil timeRangeUtil;

    // ============ REST API 接口 ============

    /**
     * 获取代币基础信息和宏观指标
     */
    @GetMapping("/v1/tokens/{tokenId}/overview")
    @Operation(summary = "获取代币概览", description = "获取代币基础信息、当前价格、市值等宏观指标")
    public ApiResponse<Map<String, Object>> getTokenOverview(
            @Parameter(description = "代币ID", example = "1")
            @PathVariable Long tokenId) {
        
        log.info("获取代币概览, tokenId: {}", tokenId);
        
        Map<String, Object> overview = new HashMap<>();
        
        // 获取代币基础信息
        var tokenInfo = tokenRepository.findTokenById(tokenId);
        if (tokenInfo != null) {
            overview.put("tokenId", tokenInfo.getTokenId());
            overview.put("symbol", tokenInfo.getSymbol());
            overview.put("name", tokenInfo.getName());
            overview.put("chainName", tokenInfo.getChainName());
            overview.put("age", tokenInfo.getAge());
            overview.put("securityScore", tokenInfo.getSecurityScore());
            overview.put("description", tokenInfo.getDescription());
            overview.put("tokenCategory", tokenInfo.getTokenCategory());
            overview.put("address", tokenInfo.getAddress());
            overview.put("decimals", tokenInfo.getDecimals());
            overview.put("issuer", tokenInfo.getIssuer());
        }
        
        // 获取代币宏观指标
        var metrics = tokenRepository.findLatestMetrics(tokenId);
        if (metrics != null) {
            Map<String, Object> metricsMap = new HashMap<>();
            metricsMap.put("currentPrice", metrics.getCurrentPrice());
            metricsMap.put("fdv", metrics.getFdv());
            metricsMap.put("mcap", metrics.getMcap());
            metricsMap.put("liquidity", metrics.getLiquidity());
            metricsMap.put("fdvMcapRatio", metrics.getFdvMcapRatio());
            metricsMap.put("mcapLiquidityRatio", metrics.getMcapLiquidityRatio());
            metricsMap.put("fdvLiquidityRatio", metrics.getFdvLiquidityRatio());
            metricsMap.put("lastUpdated", metrics.getEndTime());
            
            overview.put("metrics", metricsMap);
        }
        
        overview.put("lastUpdated", LocalDateTime.now());
        
        return ApiResponse.success(overview);
    }

    /**
     * 获取代币价格历史（用于折线图）
     */
    @GetMapping("/v1/tokens/{tokenId}/price-chart")
    @Operation(summary = "获取价格历史图表数据", description = "获取代币价格历史数据，用于绘制价格折线图")
    public ApiResponse<Map<String, Object>> getPriceChart(
            @PathVariable Long tokenId,
            @RequestParam(defaultValue = "24h") String timeRange) {
        
        log.info("获取价格图表数据, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        Map<String, Object> chartData = new HashMap<>();
        
        try {
            // 解析时间范围
            LocalDateTime endTime = LocalDateTime.now();
            var timeRangeObj = timeRangeUtil.parseTimeRange(timeRange, endTime);
            LocalDateTime startTime = timeRangeObj.getStartTime();
            
            // 获取价格历史数据
            var priceHistory = tokenRepository.findPriceHistory(tokenId, startTime, endTime);
            
            chartData.put("tokenId", tokenId);
            chartData.put("timeRange", timeRange);
            chartData.put("startTime", startTime);
            chartData.put("endTime", endTime);
            chartData.put("priceData", priceHistory);
            chartData.put("dataPoints", priceHistory.size());
            
            // 计算价格统计
            if (!priceHistory.isEmpty()) {
                var latestPrice = priceHistory.get(priceHistory.size() - 1).getPrice();
                var earliestPrice = priceHistory.get(0).getPrice();
                
                if (earliestPrice != null && latestPrice != null && earliestPrice.compareTo(BigDecimal.ZERO) > 0) {
                    var priceChange = latestPrice.subtract(earliestPrice);
                    var priceChangePercent = priceChange.divide(earliestPrice, 4, BigDecimal.ROUND_HALF_UP)
                            .multiply(BigDecimal.valueOf(100));
                    
                    chartData.put("priceChange", priceChange);
                    chartData.put("priceChangePercent", priceChangePercent);
                }
                
                chartData.put("currentPrice", latestPrice);
                chartData.put("highestPrice", priceHistory.stream()
                        .map(TokenPriceHistory::getPrice)
                        .filter(p -> p != null)
                        .max(BigDecimal::compareTo)
                        .orElse(null));
                chartData.put("lowestPrice", priceHistory.stream()
                        .map(TokenPriceHistory::getPrice)
                        .filter(p -> p != null)
                        .min(BigDecimal::compareTo)
                        .orElse(null));
            }
            
        } catch (Exception e) {
            log.error("获取价格图表数据失败", e);
            chartData.put("error", "数据获取失败: " + e.getMessage());
        }
        
        return ApiResponse.success(chartData);
    }

    /**
     * 获取代币交易流数据
     */
    @GetMapping("/v1/tokens/{tokenId}/trade-flow")
    @Operation(summary = "获取交易流数据", description = "获取DEX交易量、标签净流入等交易流数据")
    public ApiResponse<Map<String, Object>> getTradeFlow(
            @PathVariable Long tokenId,
            @RequestParam(defaultValue = "1h") String timeRange) {
        
        log.info("获取交易流数据, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        Map<String, Object> tradeFlow = new HashMap<>();
        
        try {
            // 解析时间范围
            LocalDateTime endTime = LocalDateTime.now();
            var timeRangeObj = timeRangeUtil.parseTimeRange(timeRange, endTime);
            LocalDateTime startTime = timeRangeObj.getStartTime();
            
            // 获取交易量统计（按标签分组）
            String timeWindow = getTimeWindow(timeRange);
            var volumeStats = tradeRepository.findTradeVolume(tokenId, timeWindow, "all", startTime, endTime);
            
            // 获取各标签的净流入数据
            var netFlowData = tradeRepository.calculateNetFlow(tokenId, startTime, endTime);
            
            tradeFlow.put("tokenId", tokenId);
            tradeFlow.put("timeRange", timeRange);
            tradeFlow.put("startTime", startTime);
            tradeFlow.put("endTime", endTime);
            tradeFlow.put("tradeVolume", volumeStats);
            tradeFlow.put("netFlow", netFlowData);
            
            // 计算总体统计
            if (!volumeStats.isEmpty()) {
                var totalVolume = volumeStats.stream()
                        .map(v -> v.getVolumeUsd())
                        .filter(vol -> vol != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalBuyVolume = volumeStats.stream()
                        .map(v -> v.getBuyVolumeUsd())
                        .filter(vol -> vol != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalSellVolume = volumeStats.stream()
                        .map(v -> v.getSellVolumeUsd())
                        .filter(vol -> vol != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalTxCount = volumeStats.stream()
                        .mapToInt(v -> v.getTxCount())
                        .sum();
                
                Map<String, Object> summary = new HashMap<>();
                summary.put("totalVolume", totalVolume);
                summary.put("totalBuyVolume", totalBuyVolume);
                summary.put("totalSellVolume", totalSellVolume);
                summary.put("totalTxCount", totalTxCount);
                summary.put("buyPressure", totalVolume.compareTo(BigDecimal.ZERO) > 0 ? 
                    totalBuyVolume.divide(totalVolume, 4, BigDecimal.ROUND_HALF_UP) : BigDecimal.ZERO);
                
                tradeFlow.put("summary", summary);
            }
            
            // 计算净流入汇总
            if (!netFlowData.isEmpty()) {
                Map<String, BigDecimal> netFlowSummary = new HashMap<>();
                for (var netFlow : netFlowData) {
                    String tag = netFlow.getTag();
                    netFlowSummary.merge(tag, netFlow.getNetFlowUsd(), BigDecimal::add);
                }
                tradeFlow.put("netFlowSummary", netFlowSummary);
            }
            
        } catch (Exception e) {
            log.error("获取交易流数据失败", e);
            tradeFlow.put("error", "数据获取失败: " + e.getMessage());
        }
        
        return ApiResponse.success(tradeFlow);
    }

    /**
     * 获取PnL数据
     */
    @GetMapping("/v1/tokens/{tokenId}/pnl")
    @Operation(summary = "获取PnL数据", description = "获取Top PnL、NUPL、SOPR、MVRV等PnL指标")
    public ApiResponse<Map<String, Object>> getPnL(
            @PathVariable Long tokenId,
            @RequestParam(defaultValue = "50") Integer topLimit) {
        
        log.info("获取PnL数据, tokenId: {}, topLimit: {}", tokenId, topLimit);
        
        Map<String, Object> pnlData = new HashMap<>();
        
        try {
            // 获取Top PnL信息
            var topPnLList = pnLRepository.findTopPnL(tokenId, topLimit);
            
            // 获取最新的宏观PnL指标
            var macroPnL = pnLRepository.findLatestMacroPnL(tokenId);
            
            pnlData.put("tokenId", tokenId);
            pnlData.put("topLimit", topLimit);
            pnlData.put("topPnL", topPnLList);
            pnlData.put("macroPnL", macroPnL);
            
            // 计算Top PnL统计
            if (!topPnLList.isEmpty()) {
                var totalPnL = topPnLList.stream()
                        .map(p -> p.getTotalPnlUsd())
                        .filter(pnl -> pnl != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalRealizedPnL = topPnLList.stream()
                        .map(p -> p.getRealizedPnlUsd())
                        .filter(pnl -> pnl != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalUnrealizedPnL = topPnLList.stream()
                        .map(p -> p.getUnrealizedPnlUsd())
                        .filter(pnl -> pnl != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var profitableCount = topPnLList.stream()
                        .mapToLong(p -> p.getTotalPnlUsd() != null && 
                                p.getTotalPnlUsd().compareTo(BigDecimal.ZERO) > 0 ? 1 : 0)
                        .sum();
                
                var avgStillHolding = topPnLList.stream()
                        .mapToDouble(p -> p.getStillHoldingPercent() != null ? p.getStillHoldingPercent() : 0.0)
                        .average()
                        .orElse(0.0);
                
                Map<String, Object> pnlSummary = new HashMap<>();
                pnlSummary.put("totalPnL", totalPnL);
                pnlSummary.put("totalRealizedPnL", totalRealizedPnL);
                pnlSummary.put("totalUnrealizedPnL", totalUnrealizedPnL);
                pnlSummary.put("profitableCount", profitableCount);
                pnlSummary.put("profitablePercentage", topPnLList.size() > 0 ? 
                    (double) profitableCount / topPnLList.size() * 100 : 0.0);
                pnlSummary.put("avgStillHoldingPercent", avgStillHolding);
                
                pnlData.put("summary", pnlSummary);
            }
            
            // 添加宏观指标解释
            if (macroPnL != null) {
                Map<String, Object> indicators = new HashMap<>();
                indicators.put("NUPL", Map.of(
                    "value", macroPnL.getNupl(),
                    "description", "Net Unrealized Profit/Loss - 市场整体盈亏状态",
                    "interpretation", interpretNUPL(macroPnL.getNupl())
                ));
                indicators.put("MVRV", Map.of(
                    "value", macroPnL.getMvrv(),
                    "description", "Market Value to Realized Value - 市值与实现价值比率",
                    "interpretation", interpretMVRV(macroPnL.getMvrv())
                ));
                indicators.put("SOPR", Map.of(
                    "value", macroPnL.getSopr(),
                    "description", "Spent Output Profit Ratio - 花费输出盈利比率",
                    "interpretation", interpretSOPR(macroPnL.getSopr())
                ));
                
                pnlData.put("indicators", indicators);
            }
            
        } catch (Exception e) {
            log.error("获取PnL数据失败", e);
            pnlData.put("error", "数据获取失败: " + e.getMessage());
        }
        
        return ApiResponse.success(pnlData);
    }

    /**
     * 获取代币分布数据
     */
    @GetMapping("/v1/tokens/{tokenId}/distribution")
    @Operation(summary = "获取代币分布数据", description = "获取持仓分布、标签维度数据、Top Holder等")
    public ApiResponse<Map<String, Object>> getDistribution(
            @PathVariable Long tokenId,
            @RequestParam(defaultValue = "100") Integer topHolderLimit) {
        
        log.info("获取代币分布数据, tokenId: {}, topHolderLimit: {}", tokenId, topHolderLimit);
        
        Map<String, Object> distribution = new HashMap<>();
        
        try {
            // 获取最新的代币分布数据
            var tokenDistribution = distributionRepository.findLatestDistribution(tokenId);
            
            // 获取Top Holders
            var topHolders = distributionRepository.findTopHolders(tokenId, topHolderLimit);
            
            // 获取标签维度持仓数据
            LocalDateTime endTime = LocalDateTime.now();
            var tagHoldings = distributionRepository.findTagHoldings(tokenId, endTime);
            
            distribution.put("tokenId", tokenId);
            distribution.put("topHolderLimit", topHolderLimit);
            distribution.put("distribution", tokenDistribution);
            distribution.put("topHolders", topHolders);
            distribution.put("tagHoldings", tagHoldings);
            
            // 计算宏观分布指标
            if (tokenDistribution != null) {
                Map<String, Object> macroMetrics = new HashMap<>();
                macroMetrics.put("holdersCount", tokenDistribution.getHoldersCount());
                macroMetrics.put("totalValueUsd", tokenDistribution.getTotalValueUsd());
                macroMetrics.put("medianHolderValue", tokenDistribution.getMedianHolderValueUsd());
                macroMetrics.put("avgHolderValue", tokenDistribution.getAvgHolderValueUsd());
                macroMetrics.put("top2SharePercent", tokenDistribution.getTop2SharePercent());
                macroMetrics.put("freshHolderSharePercent", tokenDistribution.getFreshHolderValueShare());
                macroMetrics.put("concentrationIndex", tokenDistribution.getConcentrationIndex());
                macroMetrics.put("concentrationLevel", getConcentrationLevel(tokenDistribution.getConcentrationIndex()));
                
                distribution.put("macroMetrics", macroMetrics);
            }
            
            // 计算Top Holders统计
            if (!topHolders.isEmpty()) {
                var top10Share = topHolders.stream()
                        .limit(10)
                        .mapToDouble(h -> h.getOwnershipPercent() != null ? h.getOwnershipPercent() : 0.0)
                        .sum();
                
                var top50Share = topHolders.stream()
                        .limit(50)
                        .mapToDouble(h -> h.getOwnershipPercent() != null ? h.getOwnershipPercent() : 0.0)
                        .sum();
                
                Map<String, Object> holderStats = new HashMap<>();
                holderStats.put("top10SharePercent", top10Share);
                holderStats.put("top50SharePercent", top50Share);
                holderStats.put("totalTopHolders", topHolders.size());
                
                // 按标签分组统计
                Map<String, Long> labelCounts = new HashMap<>();
                Map<String, Double> labelShares = new HashMap<>();
                
                for (var holder : topHolders) {
                    if (holder.getLabels() != null) {
                        for (String label : holder.getLabels()) {
                            labelCounts.merge(label, 1L, Long::sum);
                            labelShares.merge(label, 
                                holder.getOwnershipPercent() != null ? holder.getOwnershipPercent() : 0.0, 
                                Double::sum);
                        }
                    }
                }
                
                holderStats.put("labelCounts", labelCounts);
                holderStats.put("labelShares", labelShares);
                
                distribution.put("holderStats", holderStats);
            }
            
            // 标签维度统计
            if (!tagHoldings.isEmpty()) {
                var totalTagValue = tagHoldings.stream()
                        .map(t -> t.getValueUsd())
                        .filter(v -> v != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                Map<String, Object> tagStats = new HashMap<>();
                tagStats.put("totalTagValue", totalTagValue);
                tagStats.put("tagCount", tagHoldings.size());
                
                // 按价值排序的标签分布
                var sortedTags = tagHoldings.stream()
                        .sorted((a, b) -> {
                            if (a.getValueUsd() == null) return 1;
                            if (b.getValueUsd() == null) return -1;
                            return b.getValueUsd().compareTo(a.getValueUsd());
                        })
                        .limit(10)
                        .toList();
                
                tagStats.put("topTags", sortedTags);
                
                distribution.put("tagStats", tagStats);
            }
            
        } catch (Exception e) {
            log.error("获取代币分布数据失败", e);
            distribution.put("error", "数据获取失败: " + e.getMessage());
        }
        
        return ApiResponse.success(distribution);
    }

    /**
     * 获取账户信息和资产
     */
    @GetMapping("/v1/accounts/{accountId}")
    @Operation(summary = "获取账户数据", description = "获取账户信息、资产、DEX交易历史等")
    public ApiResponse<Map<String, Object>> getAccountData(
            @PathVariable Long accountId) {
        
        log.info("获取账户数据, accountId: {}", accountId);
        
        Map<String, Object> accountData = new HashMap<>();
        
        try {
            // 获取账户基础信息
            var accountInfo = accountRepository.findAccountById(accountId);
            
            // 获取账户资产
            var assets = accountRepository.findAccountAssets(accountId);
            
            // 获取最近24小时的转账历史
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = endTime.minusHours(24);
            var transferHistory = accountRepository.findTransferHistory(accountId, startTime, endTime);
            
            accountData.put("accountId", accountId);
            accountData.put("accountInfo", accountInfo);
            accountData.put("assets", assets);
            accountData.put("transferHistory", transferHistory);
            
            // 计算资产统计
            if (!assets.isEmpty()) {
                var totalValueUsd = assets.stream()
                        .map(a -> a.getValueUsd())
                        .filter(v -> v != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                // 按资产类型分组
                Map<String, BigDecimal> assetTypeValues = new HashMap<>();
                Map<String, Long> assetTypeCounts = new HashMap<>();
                
                for (var asset : assets) {
                    String type = asset.getAssetType();
                    assetTypeValues.merge(type, 
                        asset.getValueUsd() != null ? asset.getValueUsd() : BigDecimal.ZERO, 
                        BigDecimal::add);
                    assetTypeCounts.merge(type, 1L, Long::sum);
                }
                
                Map<String, Object> assetStats = new HashMap<>();
                assetStats.put("totalValueUsd", totalValueUsd);
                assetStats.put("totalAssets", assets.size());
                assetStats.put("assetTypeValues", assetTypeValues);
                assetStats.put("assetTypeCounts", assetTypeCounts);
                
                // Top 5 资产
                var top5Assets = assets.stream()
                        .sorted((a, b) -> {
                            if (a.getValueUsd() == null) return 1;
                            if (b.getValueUsd() == null) return -1;
                            return b.getValueUsd().compareTo(a.getValueUsd());
                        })
                        .limit(5)
                        .toList();
                
                assetStats.put("top5Assets", top5Assets);
                
                accountData.put("assetStats", assetStats);
            }
            
            // 计算转账历史统计
            if (!transferHistory.isEmpty()) {
                var totalTxCount = transferHistory.stream()
                        .mapToInt(h -> h.getTotalTxCount())
                        .sum();
                
                var totalBuyVolume = transferHistory.stream()
                        .map(h -> h.getBuyVolumeUsd())
                        .filter(v -> v != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var totalSellVolume = transferHistory.stream()
                        .map(h -> h.getSellVolumeUsd())
                        .filter(v -> v != null)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                
                var netBuyVolume = totalBuyVolume.subtract(totalSellVolume);
                
                Map<String, Object> transferStats = new HashMap<>();
                transferStats.put("totalTxCount", totalTxCount);
                transferStats.put("totalBuyVolume", totalBuyVolume);
                transferStats.put("totalSellVolume", totalSellVolume);
                transferStats.put("netBuyVolume", netBuyVolume);
                transferStats.put("timeWindow", "24h");
                transferStats.put("tradingActive", totalTxCount > 0);
                
                accountData.put("transferStats", transferStats);
            }
            
            // 账户标签解析
            if (accountInfo != null && accountInfo.getLabels() != null) {
                Map<String, Object> labelInfo = new HashMap<>();
                labelInfo.put("labels", accountInfo.getLabels());
                labelInfo.put("labelCount", accountInfo.getLabels().size());
                labelInfo.put("isExchange", accountInfo.getLabels().contains("Exchange"));
                labelInfo.put("isSmartMoney", accountInfo.getLabels().contains("SmartMoney"));
                labelInfo.put("isWhale", accountInfo.getLabels().contains("Whale"));
                labelInfo.put("isFresh", accountInfo.getLabels().contains("Fresh"));
                
                accountData.put("labelInfo", labelInfo);
            }
            
        } catch (Exception e) {
            log.error("获取账户数据失败", e);
            accountData.put("error", "数据获取失败: " + e.getMessage());
        }
        
        return ApiResponse.success(accountData);
    }

    // ============ WebSocket 消息处理 ============

    /**
     * 客户端订阅代币实时数据
     */
    @MessageMapping("/subscribe/token/{tokenId}")
    @SendTo("/topic/token/{tokenId}/realtime")
    public Map<String, Object> subscribeTokenRealtime(@DestinationVariable Long tokenId) {
        log.info("客户端订阅代币实时数据: {}", tokenId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("tokenId", tokenId);
        response.put("subscribed", true);
        response.put("timestamp", LocalDateTime.now());
        
        return response;
    }

    // ============ 定时推送任务 ============

    /**
     * 定时推送代币价格更新（模拟实时数据）
     */
    @Scheduled(fixedRate = 5000) // 每5秒推送一次
    public void pushRealtimePriceUpdates() {
        // 模拟推送代币1的价格更新
        Long tokenId = 1L;
        
        Map<String, Object> priceUpdate = new HashMap<>();
        priceUpdate.put("tokenId", tokenId);
        priceUpdate.put("price", generateRandomPrice());
        priceUpdate.put("change", generateRandomChange());
        priceUpdate.put("timestamp", LocalDateTime.now());
        
        webSocketService.pushTokenPriceUpdate(tokenId, priceUpdate);
    }

    /**
     * 定时推送交易流更新
     */
    @Scheduled(fixedRate = 10000) // 每10秒推送一次
    public void pushRealtimeTradeFlow() {
        Long tokenId = 1L;
        
        Map<String, Object> tradeUpdate = new HashMap<>();
        tradeUpdate.put("tokenId", tokenId);
        tradeUpdate.put("volume", generateRandomVolume());
        tradeUpdate.put("netFlow", generateRandomNetFlow());
        tradeUpdate.put("timestamp", LocalDateTime.now());
        
        webSocketService.pushTokenVolumeUpdate(tokenId, tradeUpdate);
    }


    // 辅助方法：生成随机价格
    private BigDecimal generateRandomPrice() {
        return BigDecimal.valueOf(1.0 + Math.random() * 0.1);
    }

    // 辅助方法：生成随机价格变化
    private BigDecimal generateRandomChange() {
        return BigDecimal.valueOf((Math.random() - 0.5) * 0.05);
    }

    // 辅助方法：生成随机交易量
    private BigDecimal generateRandomVolume() {
        return BigDecimal.valueOf(100000 + Math.random() * 50000);
    }

    // 辅助方法：生成随机净流入
    private BigDecimal generateRandomNetFlow() {
        return BigDecimal.valueOf((Math.random() - 0.5) * 10000);
    }

    // ============ 辅助方法 ============

    /**
     * 根据时间范围获取对应的时间窗口
     */
    private String getTimeWindow(String timeRange) {
        return switch (timeRange.toLowerCase()) {
            case "1h", "1hour" -> "1min";
            case "24h", "1d", "1day" -> "5min";
            case "7d", "1w", "1week" -> "1hour";
            case "30d", "1m", "1month" -> "1day";
            default -> "1min";
        };
    }

    /**
     * 解释NUPL指标
     */
    private String interpretNUPL(Double nupl) {
        if (nupl == null) return "数据不足";
        if (nupl > 0.75) return "极度贪婪 - 市场过热";
        if (nupl > 0.5) return "贪婪 - 市场乐观";
        if (nupl > 0.25) return "乐观 - 轻度获利";
        if (nupl > 0) return "中性偏乐观";
        if (nupl > -0.25) return "中性偏悲观";
        if (nupl > -0.5) return "恐惧 - 市场悲观";
        return "极度恐惧 - 市场恐慌";
    }

    /**
     * 解释MVRV指标
     */
    private String interpretMVRV(Double mvrv) {
        if (mvrv == null) return "数据不足";
        if (mvrv > 3.0) return "极度高估 - 考虑获利了结";
        if (mvrv > 2.0) return "高估 - 谨慎持有";
        if (mvrv > 1.5) return "轻度高估 - 正常波动";
        if (mvrv > 1.0) return "合理估值 - 持有";
        if (mvrv > 0.8) return "轻度低估 - 考虑买入";
        if (mvrv > 0.6) return "低估 - 买入机会";
        return "严重低估 - 绝佳买入机会";
    }

    /**
     * 解释SOPR指标
     */
    private String interpretSOPR(Double sopr) {
        if (sopr == null) return "数据不足";
        if (sopr > 1.05) return "获利抛售明显 - 可能面临回调";
        if (sopr > 1.02) return "轻度获利抛售 - 谨慎观察";
        if (sopr > 0.98) return "平衡状态 - 市场稳定";
        if (sopr > 0.95) return "轻度亏损抛售 - 可能筑底";
        return "恐慌性抛售 - 可能是买入机会";
    }

    /**
     * 获取集中度等级描述
     */
    private String getConcentrationLevel(Double concentrationIndex) {
        if (concentrationIndex == null) return "未知";
        if (concentrationIndex > 0.8) return "高度集中";
        if (concentrationIndex > 0.6) return "中度集中";
        if (concentrationIndex > 0.4) return "轻度集中";
        return "相对分散";
    }
}
