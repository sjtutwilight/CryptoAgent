package com.twilight.backend.repository.impl;

import com.twilight.backend.model.TokenListItem;
import com.twilight.backend.model.TokenOverview;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.TokenPnL;
import com.twilight.backend.repository.TokenRepository;
import com.twilight.backend.util.RandomValueGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 代币数据访问实现类
 */
@Slf4j
@Repository
public class TokenRepositoryImpl implements TokenRepository {

    private final JdbcTemplate postgresqlJdbcTemplate;
    private final JdbcTemplate clickhouseJdbcTemplate;
    private final RandomValueGenerator randomValueGenerator;

    public TokenRepositoryImpl(@Qualifier("postgresqlJdbcTemplate") JdbcTemplate postgresqlJdbcTemplate,
                              @Qualifier("clickhouseJdbcTemplate") JdbcTemplate clickhouseJdbcTemplate,
                              RandomValueGenerator randomValueGenerator) {
        this.postgresqlJdbcTemplate = postgresqlJdbcTemplate;
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
        this.randomValueGenerator = randomValueGenerator;
    }

    @Override
    public List<TokenListItem> findTokenListItems(Integer page, Integer pageSize, String sortBy, String order) {
        try {
            // 第一步：从ClickHouse获取最新的代币指标数据
            List<TokenMetricData> metricsData = getTokenMetricsFromClickHouse(page, pageSize, sortBy, order);
            
            if (metricsData.isEmpty()) {
                return List.of();
            }
            
            // 第二步：从PostgreSQL获取对应的代币基础信息
            List<Long> tokenIds = metricsData.stream().map(TokenMetricData::getTokenId).toList();
            List<TokenBasicInfo> basicInfoList = getTokenBasicInfoFromPostgres(tokenIds);
            
            // 第三步：合并数据构建TokenListItem
            return buildTokenListItems(metricsData, basicInfoList);
            
        } catch (Exception e) {
            log.error("获取代币列表失败, page: {}, pageSize: {}, sortBy: {}, order: {}", 
                page, pageSize, sortBy, order, e);
            return List.of();
        }
    }
    
    /**
     * 从ClickHouse获取代币指标数据
     */
    private List<TokenMetricData> getTokenMetricsFromClickHouse(Integer page, Integer pageSize, String sortBy, String order) {
        // 构建排序字段映射
        String orderByField = switch (sortBy.toLowerCase()) {
            case "mcap" -> "cur.mcap_usd";
            case "volume" -> "cur.volume_usd"; 
            case "price" -> "cur.token_price_usd";
            case "change1m" -> "price_change_pct";
            default -> "cur.mcap_usd";
        };
        
        String orderDirection = "desc".equalsIgnoreCase(order) ? "DESC" : "ASC";
        
        int offset = (page - 1) * pageSize;

        // 说明：
        // - 使用窗口函数对每个token按end_time降序排名，rn=1为最新，rn=2为上一分钟
        // - 去掉了严格的“最近10分钟”时间过滤，避免数据延迟导致全空
        // - 为避免部分ClickHouse JDBC版本对LIMIT/OFFSET参数占位符支持不佳，这里将其内联为数字常量
        String sql = """
            WITH ranked AS (
                SELECT 
                    token_id,
                    token_price_usd,
                    mcap_usd,
                    liquidity_usd,
                    volume_usd,
                    buy_volume_usd,
                    sell_volume_usd,
                    (buy_volume_usd - sell_volume_usd) AS buy_pressure_diff,
                    end_time,
                    ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY end_time DESC) AS rn
                FROM token_recent_metric_ch
                WHERE tag = 'all' AND time_window = '1min'
            )
            SELECT 
                cur.token_id,
                cur.token_price_usd AS price,
                cur.mcap_usd,
                cur.liquidity_usd,
                cur.volume_usd,
                cur.buy_volume_usd,
                cur.sell_volume_usd,
                cur.buy_pressure_diff,
                CASE 
                    WHEN prev.token_price_usd > 0 THEN 
                        ((cur.token_price_usd - prev.token_price_usd) / prev.token_price_usd * 100)
                    ELSE 0 
                END AS price_change_pct
            FROM ranked cur
            LEFT JOIN ranked prev ON cur.token_id = prev.token_id AND prev.rn = 2
            WHERE cur.rn = 1
              AND cur.token_price_usd > 0 
              AND cur.mcap_usd > 0
            ORDER BY %s %s
            LIMIT %d OFFSET %d
            """.formatted(orderByField, orderDirection, pageSize, offset);

        return clickhouseJdbcTemplate.query(sql, new TokenMetricDataRowMapper());
    }
    
    /**
     * 从PostgreSQL获取代币基础信息
     */
    private List<TokenBasicInfo> getTokenBasicInfoFromPostgres(List<Long> tokenIds) {
        if (tokenIds.isEmpty()) {
            return List.of();
        }
        
        // 构建IN子句的占位符
        String placeholders = tokenIds.stream().map(id -> "?").reduce((a, b) -> a + "," + b).orElse("");
        
        String sql = """
            SELECT id as token_id, token_symbol as symbol, chain_name
            FROM token 
            WHERE id IN (%s)
            """.formatted(placeholders);
        
        return postgresqlJdbcTemplate.query(sql, new TokenBasicInfoRowMapper(), tokenIds.toArray());
    }
    
    /**
     * 合并数据构建TokenListItem列表
     */
    private List<TokenListItem> buildTokenListItems(List<TokenMetricData> metricsData, List<TokenBasicInfo> basicInfoList) {
        // 创建基础信息映射表，便于快速查找
        java.util.Map<Long, TokenBasicInfo> basicInfoMap = basicInfoList.stream()
            .collect(java.util.stream.Collectors.toMap(TokenBasicInfo::getTokenId, info -> info));
        
        DecimalFormat priceFormat = new DecimalFormat("#,##0.####");
        DecimalFormat volumeFormat = new DecimalFormat("#,##0");
        DecimalFormat changeFormat = new DecimalFormat("+#,##0.##;-#,##0.##");
        
        return metricsData.stream()
            .map(metrics -> {
                TokenBasicInfo basicInfo = basicInfoMap.get(metrics.getTokenId());
                if (basicInfo == null) {
                    return null; // 跳过没有基础信息的代币
                }
                
                TokenListItem item = new TokenListItem();
                
                // 基础信息
                item.setTokenId(metrics.getTokenId());
                item.setSymbol(basicInfo.getSymbol());
                item.setName(basicInfo.getSymbol()); // name使用symbol
                item.setChainName(basicInfo.getChainName());
                
                // 格式化价格
                item.setPrice(metrics.getPrice() != null ? priceFormat.format(metrics.getPrice()) : "0");
                
                // 格式化涨跌幅
                item.setChange1m(metrics.getPriceChangePct() != null ? changeFormat.format(metrics.getPriceChangePct()) : "0");
                
                // 格式化市值
                item.setMcap(metrics.getMcapUsd() != null ? volumeFormat.format(metrics.getMcapUsd()) : "0");
                
                // 格式化交易量
                item.setDexVolume(metrics.getVolumeUsd() != null ? volumeFormat.format(metrics.getVolumeUsd()) : "0");
                
                // 格式化流动性
                item.setLiquidity(metrics.getLiquidityUsd() != null ? volumeFormat.format(metrics.getLiquidityUsd()) : "0");
                
                // 格式化买入量
                item.setDexBuys(metrics.getBuyVolumeUsd() != null ? volumeFormat.format(metrics.getBuyVolumeUsd()) : "0");
                
                // 格式化卖出量
                item.setDexSells(metrics.getSellVolumeUsd() != null ? volumeFormat.format(metrics.getSellVolumeUsd()) : "0");
                
                // 计算买入压力（buyVolume - sellVolume），转换为比例
                if (metrics.getBuyPressureDiff() != null && metrics.getVolumeUsd() != null && 
                    metrics.getVolumeUsd().compareTo(BigDecimal.ZERO) > 0) {
                    // 将差值转换为0-1的比例：(buyVolume - sellVolume) / totalVolume * 0.5 + 0.5
                    double ratio = metrics.getBuyPressureDiff().divide(metrics.getVolumeUsd(), 4, RoundingMode.HALF_UP).doubleValue() * 0.5 + 0.5;
                    // 限制在0-1范围内
                    ratio = Math.max(0.0, Math.min(1.0, ratio));
                    item.setBuyPressure(ratio);
                } else {
                    item.setBuyPressure(0.5); // 默认中性
                }
                
                return item;
            })
            .filter(item -> item != null) // 过滤掉null项
            .toList();
    }

    @Override
    public Long countTokenListItems() {
        String sql = """
            SELECT count()
            FROM (
                SELECT token_id
                FROM (
                    SELECT 
                        token_id,
                        token_price_usd,
                        mcap_usd,
                        ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY end_time DESC) AS rn
                    FROM token_recent_metric_ch
                    WHERE tag = 'all' AND time_window = '1min'
                ) ranked
                WHERE rn = 1 AND token_price_usd > 0 AND mcap_usd > 0
                GROUP BY token_id
            )
            """;
        
        try {
            Long count = clickhouseJdbcTemplate.queryForObject(sql, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            log.error("获取代币列表总数失败", e);
            return 0L;
        }
    }

    /**
     * 代币指标数据内部类
     */
    private static class TokenMetricData {
        private Long tokenId;
        private BigDecimal price;
        private BigDecimal mcapUsd;
        private BigDecimal liquidityUsd;
        private BigDecimal volumeUsd;
        private BigDecimal buyVolumeUsd;
        private BigDecimal sellVolumeUsd;
        private BigDecimal buyPressureDiff;
        private BigDecimal priceChangePct;
        
        // Getters and Setters
        public Long getTokenId() { return tokenId; }
        public void setTokenId(Long tokenId) { this.tokenId = tokenId; }
        public BigDecimal getPrice() { return price; }
        public void setPrice(BigDecimal price) { this.price = price; }
        public BigDecimal getMcapUsd() { return mcapUsd; }
        public void setMcapUsd(BigDecimal mcapUsd) { this.mcapUsd = mcapUsd; }
        public BigDecimal getLiquidityUsd() { return liquidityUsd; }
        public void setLiquidityUsd(BigDecimal liquidityUsd) { this.liquidityUsd = liquidityUsd; }
        public BigDecimal getVolumeUsd() { return volumeUsd; }
        public void setVolumeUsd(BigDecimal volumeUsd) { this.volumeUsd = volumeUsd; }
        public BigDecimal getBuyVolumeUsd() { return buyVolumeUsd; }
        public void setBuyVolumeUsd(BigDecimal buyVolumeUsd) { this.buyVolumeUsd = buyVolumeUsd; }
        public BigDecimal getSellVolumeUsd() { return sellVolumeUsd; }
        public void setSellVolumeUsd(BigDecimal sellVolumeUsd) { this.sellVolumeUsd = sellVolumeUsd; }
        public BigDecimal getBuyPressureDiff() { return buyPressureDiff; }
        public void setBuyPressureDiff(BigDecimal buyPressureDiff) { this.buyPressureDiff = buyPressureDiff; }
        public BigDecimal getPriceChangePct() { return priceChangePct; }
        public void setPriceChangePct(BigDecimal priceChangePct) { this.priceChangePct = priceChangePct; }
    }
    
    /**
     * 代币基础信息内部类
     */
    private static class TokenBasicInfo {
        private Long tokenId;
        private String symbol;
        private String chainName;
        
        // Getters and Setters
        public Long getTokenId() { return tokenId; }
        public void setTokenId(Long tokenId) { this.tokenId = tokenId; }
        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public String getChainName() { return chainName; }
        public void setChainName(String chainName) { this.chainName = chainName; }
    }

    /**
     * TokenMetricData行映射器
     */
    private class TokenMetricDataRowMapper implements RowMapper<TokenMetricData> {
        @Override
        public TokenMetricData mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenMetricData data = new TokenMetricData();
            data.setTokenId(rs.getLong("token_id"));
            data.setPrice(rs.getBigDecimal("price"));
            data.setMcapUsd(rs.getBigDecimal("mcap_usd"));
            data.setLiquidityUsd(rs.getBigDecimal("liquidity_usd"));
            data.setVolumeUsd(rs.getBigDecimal("volume_usd"));
            data.setBuyVolumeUsd(rs.getBigDecimal("buy_volume_usd"));
            data.setSellVolumeUsd(rs.getBigDecimal("sell_volume_usd"));
            data.setBuyPressureDiff(rs.getBigDecimal("buy_pressure_diff"));
            data.setPriceChangePct(rs.getBigDecimal("price_change_pct"));
            return data;
        }
    }
    
    /**
     * TokenBasicInfo行映射器
     */
    private class TokenBasicInfoRowMapper implements RowMapper<TokenBasicInfo> {
        @Override
        public TokenBasicInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            TokenBasicInfo info = new TokenBasicInfo();
            info.setTokenId(rs.getLong("token_id"));
            info.setSymbol(rs.getString("symbol"));
            info.setChainName(rs.getString("chain_name"));
            return info;
        }
    }

    

    @Override
    public TokenOverview findTokenOverview(Long tokenId, String timeRange) {
        try {
            log.info("查询代币概览, tokenId: {}, timeRange: {}", tokenId, timeRange);
            
            // 1. 获取基础信息（从PostgreSQL）
            TokenOverview.BasicInfo basicInfo = getBasicInfo(tokenId);
            if (basicInfo == null) {
                log.warn("代币基础信息不存在, tokenId: {}", tokenId);
                return null;
            }
            
            // 2. 获取宏观指标（从ClickHouse最新数据）
            TokenOverview.Metrics metrics = getLatestMetrics(tokenId);
            
            // 3. 获取交易流分析（从ClickHouse聚合数据）
            TokenOverview.TradeFlow tradeFlow = getTradeFlowAnalysis(tokenId, timeRange);
            
            // 4. 获取价格走势图表
            TokenOverview.PriceChart priceChart = getPriceChart(tokenId, timeRange);
            
            // 5. 获取Top买卖地址
            TokenOverview.TopTraders topTraders = getTopTraders(tokenId, timeRange);
            
            // 6. 获取交易明细
            List<TokenOverview.TradeDetail> recentTrades = getRecentTrades(tokenId, timeRange);
            
            // 6. 组装结果
            TokenOverview overview = new TokenOverview();
            overview.setTokenId(tokenId);
            overview.setTimeRange(timeRange);
            overview.setBasicInfo(basicInfo);
            overview.setMetrics(metrics);
            overview.setTradeFlow(tradeFlow);
            overview.setPriceChart(priceChart);
            overview.setTopTraders(topTraders);
            overview.setRecentTrades(recentTrades);
            overview.setWindowTime(resolveTimeWindow(timeRange));
            
            return overview;
            
        } catch (Exception e) {
            log.error("获取代币概览失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return null;
        }
    }
    
    /**
     * 获取代币基础信息
     */
    private TokenOverview.BasicInfo getBasicInfo(Long tokenId) {
        String sql = """
            SELECT token_symbol, chain_name, token_catagory, token_decimals, token_address, issuer
            FROM token 
            WHERE id = ?
            """;
        
        try {
            return postgresqlJdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                TokenOverview.BasicInfo info = new TokenOverview.BasicInfo();
                
                String symbol = rs.getString("token_symbol");
                info.setSymbol(symbol);
                info.setName(symbol); // name使用symbol
                info.setChainName(rs.getString("chain_name"));
                info.setTokenCategory(rs.getString("token_catagory"));
                info.setDecimals(rs.getInt("token_decimals"));
                info.setAddress(rs.getString("token_address"));
                info.setIssuer(rs.getString("issuer"));
                
                // 生成随机值（基于tokenId种子确保一致性）
                info.setAge(randomValueGenerator.generateAge(tokenId));
                info.setSecurityScore(randomValueGenerator.generateSecurityScore(tokenId));
                info.setDescription(generateDescription(symbol, info.getTokenCategory()));
                
                return info;
            }, tokenId);
            
        } catch (Exception e) {
            log.error("获取代币基础信息失败, tokenId: {}", tokenId, e);
            return null;
        }
    }
    
    /**
     * 获取最新宏观指标
     */
    private TokenOverview.Metrics getLatestMetrics(Long tokenId) {
        String sql = """
            WITH ranked AS (
                SELECT 
                    token_id,
                    token_price_usd,
                    mcap_usd,
                    fdv_usd,
                    liquidity_usd,
                    ROW_NUMBER() OVER (PARTITION BY token_id ORDER BY end_time DESC) AS rn
                FROM token_recent_metric_ch
                WHERE tag = 'all' AND time_window = '1min' AND token_id = ?
            ),
            previous AS (
                SELECT 
                    token_price_usd as prev_price
                FROM ranked 
                WHERE rn = 2
            )
            SELECT 
                cur.token_price_usd,
                cur.mcap_usd,
                cur.fdv_usd,
                cur.liquidity_usd,
                CASE 
                    WHEN prev.prev_price > 0 THEN 
                        ((cur.token_price_usd - prev.prev_price) / prev.prev_price * 100)
                    ELSE 0 
                END AS price_change_pct,
                CASE 
                    WHEN cur.mcap_usd > 0 THEN cur.fdv_usd / cur.mcap_usd
                    ELSE 0 
                END AS fdv_mcap_ratio,
                CASE 
                    WHEN cur.liquidity_usd > 0 THEN cur.mcap_usd / cur.liquidity_usd
                    ELSE 0 
                END AS mcap_liquidity_ratio,
                CASE 
                    WHEN cur.liquidity_usd > 0 THEN cur.fdv_usd / cur.liquidity_usd
                    ELSE 0 
                END AS fdv_liquidity_ratio
            FROM ranked cur
            LEFT JOIN previous prev ON 1=1
            WHERE cur.rn = 1
            """;
        
        try {
            List<TokenOverview.Metrics> results = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.Metrics metrics = new TokenOverview.Metrics();
                
                BigDecimal price = rs.getBigDecimal("token_price_usd");
                metrics.setCurrentPrice(price != null ? price.toString() : "0");
                
                BigDecimal changePct = rs.getBigDecimal("price_change_pct");
                metrics.setPriceChangePercent(changePct != null ? changePct.toString() : "0");
                
                BigDecimal mcap = rs.getBigDecimal("mcap_usd");
                metrics.setMcap(mcap != null ? mcap.toString() : "0");
                
                BigDecimal fdv = rs.getBigDecimal("fdv_usd");
                metrics.setFdv(fdv != null ? fdv.toString() : "0");
                
                BigDecimal liquidity = rs.getBigDecimal("liquidity_usd");
                metrics.setLiquidity(liquidity != null ? liquidity.toString() : "0");
                
                metrics.setFdvMcapRatio(rs.getDouble("fdv_mcap_ratio"));
                metrics.setMcapLiquidityRatio(rs.getDouble("mcap_liquidity_ratio"));
                metrics.setFdvLiquidityRatio(rs.getDouble("fdv_liquidity_ratio"));
                
                return metrics;
            }, tokenId);
            
            return results.isEmpty() ? new TokenOverview.Metrics() : results.get(0);
            
        } catch (Exception e) {
            log.error("获取代币宏观指标失败, tokenId: {}", tokenId, e);
            return new TokenOverview.Metrics();
        }
    }
    
    /**
     * 获取交易流分析
     */
    private TokenOverview.TradeFlow getTradeFlowAnalysis(Long tokenId, String timeRange) {
        try {
            // 获取交易汇总统计
            TokenOverview.Summary summary = getTradeSummary(tokenId, timeRange);
            
            // 获取标签流向数据
            List<TokenOverview.TagFlowDetail> tagFlowDetails = getTagFlowDetails(tokenId, timeRange);
            
            // 计算标签净流入汇总
            Map<String, String> netFlowSummary = calculateNetFlowSummary(tagFlowDetails);
            
            TokenOverview.TradeFlow tradeFlow = new TokenOverview.TradeFlow();
            tradeFlow.setSummary(summary);
            tradeFlow.setNetFlowSummary(netFlowSummary);
            tradeFlow.setTagFlowDetails(tagFlowDetails);
            
            return tradeFlow;
            
        } catch (Exception e) {
            log.error("获取交易流分析失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new TokenOverview.TradeFlow();
        }
    }
    
    /**
     * 获取交易汇总统计
     */
    private TokenOverview.Summary getTradeSummary(Long tokenId, String timeRange) {
        String timeWindow = resolveTimeWindow(timeRange);
        
        String sql = """
            SELECT 
                volume_usd as total_volume,
                buy_volume_usd as total_buy_volume,
                sell_volume_usd as total_sell_volume,
                txcnt as total_tx_count,
                buy_count as buy_tx_count,
                sell_count as sell_tx_count
            FROM token_recent_metric_ch
            WHERE token_id = ? AND tag = 'all' AND time_window = ?
            ORDER BY end_time DESC
            LIMIT 1
            """;
        
        try {
            List<TokenOverview.Summary> results = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.Summary summary = new TokenOverview.Summary();
                
                BigDecimal totalVolume = rs.getBigDecimal("total_volume");
                summary.setTotalVolume(totalVolume != null ? totalVolume.toString() : "0");
                
                BigDecimal totalBuyVolume = rs.getBigDecimal("total_buy_volume");
                summary.setTotalBuyVolume(totalBuyVolume != null ? totalBuyVolume.toString() : "0");
                
                BigDecimal totalSellVolume = rs.getBigDecimal("total_sell_volume");
                summary.setTotalSellVolume(totalSellVolume != null ? totalSellVolume.toString() : "0");
                
                summary.setTotalTxCount(rs.getInt("total_tx_count"));
                summary.setBuyTxCount(rs.getInt("buy_tx_count"));
                summary.setSellTxCount(rs.getInt("sell_tx_count"));
                
                // 计算买入压力
                if (totalVolume != null && totalVolume.compareTo(BigDecimal.ZERO) > 0 && totalBuyVolume != null) {
                    double buyPressure = totalBuyVolume.divide(totalVolume, 4, RoundingMode.HALF_UP).doubleValue();
                    summary.setBuyPressure(buyPressure);
                } else {
                    summary.setBuyPressure(0.5);
                }
                
                return summary;
            }, tokenId, timeWindow);
            
            return results.isEmpty() ? new TokenOverview.Summary() : results.get(0);
            
        } catch (Exception e) {
            log.error("获取交易汇总统计失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new TokenOverview.Summary();
        }
    }
    
    /**
     * 获取标签流向详情
     */
    private List<TokenOverview.TagFlowDetail> getTagFlowDetails(Long tokenId, String timeRange) {
        String timeWindow = resolveTimeWindow(timeRange);
        
        String sql = """
            SELECT
                tag,
                buy_volume_usd - sell_volume_usd as net_flow_usd,
                buy_volume_usd,
                sell_volume_usd,
                txcnt as tx_count,
                buy_count as buy_tx_count,
                sell_count as sell_tx_count,
                if(txcnt > 0, volume_usd / txcnt, 0) as avg_tx_size
            FROM (
                SELECT
                    tag,
                    volume_usd,
                    buy_volume_usd,
                    sell_volume_usd,
                    txcnt,
                    buy_count,
                    sell_count,
                    ROW_NUMBER() OVER (PARTITION BY tag ORDER BY end_time DESC) as rn
                FROM token_recent_metric_ch
                WHERE token_id = ? AND time_window = ? AND tag != 'all'
                ORDER BY end_time DESC
            ) recent
            WHERE rn = 1
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.TagFlowDetail detail = new TokenOverview.TagFlowDetail();
                
                detail.setTag(rs.getString("tag"));
                detail.setTimeWindow(timeRange);
                
                BigDecimal netFlow = rs.getBigDecimal("net_flow_usd");
                detail.setNetFlowUsd(netFlow != null ? netFlow.toString() : "0");
                
                BigDecimal buyVolume = rs.getBigDecimal("buy_volume_usd");
                detail.setBuyVolumeUsd(buyVolume != null ? buyVolume.toString() : "0");
                
                BigDecimal sellVolume = rs.getBigDecimal("sell_volume_usd");
                detail.setSellVolumeUsd(sellVolume != null ? sellVolume.toString() : "0");
                
                detail.setTxCount(rs.getInt("tx_count"));
                detail.setBuyTxCount(rs.getInt("buy_tx_count"));
                detail.setSellTxCount(rs.getInt("sell_tx_count"));
                
                BigDecimal avgTxSize = rs.getBigDecimal("avg_tx_size");
                detail.setAvgTxSize(avgTxSize != null ? avgTxSize.toString() : "0");
                
                return detail;
            }, tokenId, timeWindow);
            
        } catch (Exception e) {
            log.error("获取标签流向详情失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return List.of();
        }
    }
    
    /**
     * 计算标签净流入汇总
     */
    private Map<String, String> calculateNetFlowSummary(List<TokenOverview.TagFlowDetail> tagFlowDetails) {
        return tagFlowDetails.stream()
            .collect(java.util.stream.Collectors.toMap(
                TokenOverview.TagFlowDetail::getTag,
                TokenOverview.TagFlowDetail::getNetFlowUsd
            ));
    }

    /**
     * 根据时间范围解析对应的聚合窗口
     */
    private String resolveTimeWindow(String timeRange) {
        if (timeRange == null) {
            return "1min";
        }

        return switch (timeRange.toLowerCase()) {
            case "20s" -> "20s";
            case "1min" -> "1min";
            case "5min" -> "5min";
            case "1h" -> "1h";
            default -> "1min";
        };
    }
    
    /**
     * 生成简单的代币描述
     */
    private String generateDescription(String symbol, String category) {
        if (symbol == null) return "暂无描述";
        
        String baseDesc = symbol + "是一个";
        
        if ("stablecoin".equalsIgnoreCase(category)) {
            baseDesc += "稳定币";
        } else if ("defi".equalsIgnoreCase(category)) {
            baseDesc += "DeFi代币";
        } else if ("meme".equalsIgnoreCase(category)) {
            baseDesc += "Meme代币";
        } else {
            baseDesc += "数字资产";
        }
        
        baseDesc += "，在区块链生态中发挥重要作用。";
        
        return baseDesc;
    }
    
    /**
     * 获取价格走势图表数据
     */
    private TokenOverview.PriceChart getPriceChart(Long tokenId, String timeRange) {
        try {
            // 固定获取20s时间窗口的最近15条数据
            String sql = """
                SELECT 
                    end_time,
                    token_price_usd,
                    volume_usd
                FROM token_recent_metric_ch
                WHERE token_id = ? 
                  AND tag = 'all' 
                  AND time_window = '20s'
                ORDER BY end_time DESC
                LIMIT 15
                """;
            
            List<TokenOverview.PriceData> priceDataList = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.PriceData priceData = new TokenOverview.PriceData();
                
                priceData.setTimestamp(rs.getTimestamp("end_time").toString());
                
                BigDecimal price = rs.getBigDecimal("token_price_usd");
                priceData.setPrice(price != null ? price.toString() : "0");
                
                BigDecimal volume = rs.getBigDecimal("volume_usd");
                priceData.setVolume(volume != null ? volume.toString() : "0");
                
                return priceData;
            }, tokenId);
            
            // 按时间正序排列（图表需要从早到晚）
            priceDataList = priceDataList.stream()
                .sorted((a, b) -> a.getTimestamp().compareTo(b.getTimestamp()))
                .toList();
            
            // 计算价格统计信息
            BigDecimal currentPrice = BigDecimal.ZERO;
            BigDecimal firstPrice = BigDecimal.ZERO;
            BigDecimal highestPrice = BigDecimal.ZERO;
            BigDecimal lowestPrice = new BigDecimal("999999999");
            
            if (!priceDataList.isEmpty()) {
                // 当前价格（最新的）
                currentPrice = new BigDecimal(priceDataList.get(priceDataList.size() - 1).getPrice());
                // 第一个价格
                firstPrice = new BigDecimal(priceDataList.get(0).getPrice());
                
                // 计算最高价和最低价
                for (TokenOverview.PriceData data : priceDataList) {
                    BigDecimal price = new BigDecimal(data.getPrice());
                    if (price.compareTo(highestPrice) > 0) {
                        highestPrice = price;
                    }
                    if (price.compareTo(lowestPrice) < 0) {
                        lowestPrice = price;
                    }
                }
            }
            
            // 计算价格变化百分比
            BigDecimal priceChangePercent = BigDecimal.ZERO;
            if (firstPrice.compareTo(BigDecimal.ZERO) > 0) {
                priceChangePercent = currentPrice.subtract(firstPrice)
                    .divide(firstPrice, 4, RoundingMode.HALF_UP)
                    .multiply(new BigDecimal("100"));
            }
            
            TokenOverview.PriceChart priceChart = new TokenOverview.PriceChart();
            priceChart.setInterval("20s");
            priceChart.setDataPoints(15);
            priceChart.setPriceData(priceDataList);
            priceChart.setCurrentPrice(currentPrice.toString());
            priceChart.setPriceChangePercent(priceChangePercent.toString());
            priceChart.setChange(priceChangePercent.toString());
            priceChart.setHighestPrice(highestPrice.toString());
            priceChart.setLowestPrice(lowestPrice.toString());
            priceChart.setTimeRange("5min"); // 15个20s数据点 = 5分钟
            
            return priceChart;
            
        } catch (Exception e) {
            log.error("获取价格走势图表失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new TokenOverview.PriceChart();
        }
    }
    
    /**
     * 获取Top买卖地址
     */
    private TokenOverview.TopTraders getTopTraders(Long tokenId, String timeRange) {
        try {
            String timeWindow = resolveTimeWindow(timeRange);
            
            // 获取Top买家
            List<TokenOverview.TopTrader> topBuyers = getTopBuyers(tokenId, timeWindow);
            
            // 获取Top卖家
            List<TokenOverview.TopTrader> topSellers = getTopSellers(tokenId, timeWindow);
            
            TokenOverview.TopTraders topTraders = new TokenOverview.TopTraders();
            topTraders.setTopBuyers(topBuyers);
            topTraders.setTopSellers(topSellers);
            
            return topTraders;
            
        } catch (Exception e) {
            log.error("获取Top买卖地址失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new TokenOverview.TopTraders();
        }
    }
    
    /**
     * 获取Top买家
     */
    private List<TokenOverview.TopTrader> getTopBuyers(Long tokenId, String timeWindow) {
        String sql = """
            SELECT 
                account_id,
                account_address,
                sum(volume_usd) as total_buy_volume,
                count() as buy_tx_count,
                avg(volume_usd) as avg_buy_size,
                max(end_time) as last_buy_time
            FROM ch_account_trade_minute
            WHERE token_id = ? 
              AND side = 'buy'
              AND end_time >= now() - INTERVAL 9 HOUR
            GROUP BY account_id, account_address
            ORDER BY total_buy_volume DESC
            LIMIT 10
            """;
        
        try {
            List<TokenOverview.TopTrader> buyers = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.TopTrader trader = new TokenOverview.TopTrader();
                
                trader.setRank(rowNum + 1);
                trader.setAccountId(rs.getLong("account_id"));
                trader.setAddress(rs.getString("account_address"));
                trader.setLabels(generateLabels(rs.getLong("account_id")));
                
                BigDecimal buyVolume = rs.getBigDecimal("total_buy_volume");
                trader.setTotalBuyVolume(buyVolume != null ? buyVolume.toString() : "0");
                trader.setTotalSellVolume("0");
                
                trader.setBuyTxCount(rs.getInt("buy_tx_count"));
                trader.setSellTxCount(0);
                
                BigDecimal avgBuySize = rs.getBigDecimal("avg_buy_size");
                trader.setAvgBuySize(avgBuySize != null ? avgBuySize.toString() : "0");
                trader.setAvgSellSize("0");
                
                trader.setLastBuyTime(rs.getTimestamp("last_buy_time").toString());
                trader.setLastSellTime(null);
                
                // 生成随机盈利率
                trader.setProfitability(generateProfitability(rs.getLong("account_id")));
                trader.setReason(null);
                
                return trader;
            }, tokenId);
            
            return buyers;
            
        } catch (Exception e) {
            log.error("获取Top买家失败, tokenId: {}, timeWindow: {}", tokenId, timeWindow, e);
            return List.of();
        }
    }
    
    /**
     * 获取Top卖家
     */
    private List<TokenOverview.TopTrader> getTopSellers(Long tokenId, String timeWindow) {
        String sql = """
            SELECT 
                account_id,
                account_address,
                sum(volume_usd) as total_sell_volume,
                count() as sell_tx_count,
                avg(volume_usd) as avg_sell_size,
                max(end_time) as last_sell_time
            FROM ch_account_trade_minute
            WHERE token_id = ? 
              AND side = 'sell'
              AND end_time >= now() - INTERVAL 9 HOUR
            GROUP BY account_id, account_address
            ORDER BY total_sell_volume DESC
            LIMIT 10
            """;
        
        try {
            List<TokenOverview.TopTrader> sellers = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.TopTrader trader = new TokenOverview.TopTrader();
                
                trader.setRank(rowNum + 1);
                trader.setAccountId(rs.getLong("account_id"));
                trader.setAddress(rs.getString("account_address"));
                trader.setLabels(generateLabels(rs.getLong("account_id")));
                
                trader.setTotalBuyVolume("0");
                BigDecimal sellVolume = rs.getBigDecimal("total_sell_volume");
                trader.setTotalSellVolume(sellVolume != null ? sellVolume.toString() : "0");
                
                trader.setBuyTxCount(0);
                trader.setSellTxCount(rs.getInt("sell_tx_count"));
                
                trader.setAvgBuySize("0");
                BigDecimal avgSellSize = rs.getBigDecimal("avg_sell_size");
                trader.setAvgSellSize(avgSellSize != null ? avgSellSize.toString() : "0");
                
                trader.setLastBuyTime(null);
                trader.setLastSellTime(rs.getTimestamp("last_sell_time").toString());
                
                trader.setProfitability(null);
                // 生成随机卖出原因
                trader.setReason(generateSellReason(rs.getLong("account_id")));
                
                return trader;
            }, tokenId);
            
            return sellers;
            
        } catch (Exception e) {
            log.error("获取Top卖家失败, tokenId: {}, timeWindow: {}", tokenId, timeWindow, e);
            return List.of();
        }
    }
    
    /**
     * 获取交易明细
     */
    private List<TokenOverview.TradeDetail> getRecentTrades(Long tokenId, String timeRange) {
        String sql = """
            SELECT 
                block_time,
                account_id,
                account_address,
                side,
                qty,
                price_usd,
                value_usd,
                tx_hash
            FROM v_token_trades_detail
            WHERE token_id = ?
              AND block_time >= now() - INTERVAL 9 HOUR
            ORDER BY block_time DESC
            LIMIT 10
            """;
        
        try {
            return clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenOverview.TradeDetail trade = new TokenOverview.TradeDetail();
                
                trade.setTimestamp(rs.getTimestamp("block_time").toString());
                trade.setAccountId(rs.getLong("account_id"));
                trade.setAddress(rs.getString("account_address"));
                trade.setLabels(generateLabels(rs.getLong("account_id")));
                trade.setAction(rs.getString("side"));
                
                BigDecimal qty = rs.getBigDecimal("qty");
                trade.setAmount(qty != null ? qty.toString() : "0");
                
                BigDecimal value = rs.getBigDecimal("value_usd");
                trade.setValue(value != null ? value.toString() : "0");
                
                BigDecimal price = rs.getBigDecimal("price_usd");
                trade.setPrice(price != null ? price.toString() : "0");
                
                trade.setTxHash(rs.getString("tx_hash"));
                
                return trade;
            }, tokenId);
            
        } catch (Exception e) {
            log.error("获取交易明细失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return List.of();
        }
    }
    
    /**
     * 生成账户地址（基于account_id）
     */
    private String generateAccountAddress(Long accountId) {
        return String.format("0x%040x", accountId);
    }
    
    /**
     * 生成标签列表（基于account_id）
     */
    private List<String> generateLabels(Long accountId) {
        String[] allLabels = {"whale", "smart", "cex", "fresh", "public"};
        int index = (int) (accountId % allLabels.length);
        return List.of(allLabels[index]);
    }
    
    /**
     * 生成盈利率（基于account_id）
     */
    private Double generateProfitability(Long accountId) {
        // 基于账户ID生成0-100之间的盈利率
        return 20.0 + (accountId % 80);
    }
    
    /**
     * 生成卖出原因
     */
    private String generateSellReason(Long accountId) {
        String[] reasons = {"止盈", "止损", "套利", "获利了结", "资金需求"};
        int index = (int) (accountId % reasons.length);
        return reasons[index];
    }
    
    @Override
    public TokenDistribution findTokenDistribution(Long tokenId, String timeRange) {
        log.info("获取代币分布, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        try {
            // 1. 获取持有者统计
            TokenDistribution.HolderStats holderStats = getHolderStats(tokenId);
            
            // 2. 获取标签分布
            List<TokenDistribution.TagDistribution> tagDistribution = getTagDistribution(tokenId, timeRange);
            
            // 3. 获取Top持币地址
            List<TokenDistribution.TopHolder> topHolders = getTopHolders(tokenId);
            
            // 4. 组装结果
            TokenDistribution distribution = new TokenDistribution();
            distribution.setTokenId(tokenId);
            distribution.setTimeRange(timeRange);
            distribution.setHolderStats(holderStats);
            distribution.setTagDistribution(tagDistribution);
            distribution.setTopHolders(topHolders);
            
            return distribution;
            
        } catch (Exception e) {
            log.error("获取代币分布失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new TokenDistribution();
        }
    }
    
    /**
     * 获取持有者统计
     */
    private TokenDistribution.HolderStats getHolderStats(Long tokenId) {
        try {
            String sql = """
                SELECT 
                    holders_count,
                    total_value_usd,
                    median_holder_value_usd,
                    avg_holder_value_usd,
                    top2_value_usd,
                    top2_share,
                    fresh_holder_value_share
                FROM v_token_distribution_minute
                WHERE token_id = ?
                ORDER BY end_time DESC
                LIMIT 1
                """;
            
            return clickhouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                TokenDistribution.HolderStats holderStats = new TokenDistribution.HolderStats();
                
                holderStats.setHoldersCount(rs.getLong("holders_count"));
                
                BigDecimal totalValue = rs.getBigDecimal("total_value_usd");
                holderStats.setTotalValueUsd(totalValue != null ? totalValue.toString() : "0");
                
                BigDecimal medianValue = rs.getBigDecimal("median_holder_value_usd");
                holderStats.setMedianHolderValueUsd(medianValue != null ? medianValue.toString() : "0");
                
                BigDecimal avgValue = rs.getBigDecimal("avg_holder_value_usd");
                holderStats.setAvgHolderValueUsd(avgValue != null ? avgValue.toString() : "0");
                
                // top2Share百分比
                Double top2Share = rs.getDouble("top2_share");
                holderStats.setTop2SharePercent(top2Share != null ? top2Share * 100 : 0.0);
                
                // 生成集中度指数（基于top2Share计算）
                Double concentrationIndex = top2Share != null ? Math.min(top2Share * 1.5, 1.0) : 0.5;
                holderStats.setConcentrationIndex(concentrationIndex);
                
                // 根据集中度指数确定集中度级别
                String concentrationLevel;
                if (concentrationIndex < 0.3) {
                    concentrationLevel = "分散";
                } else if (concentrationIndex < 0.7) {
                    concentrationLevel = "中度集中";
                } else {
                    concentrationLevel = "高度集中";
                }
                holderStats.setConcentrationLevel(concentrationLevel);
                
                // 从数据库获取fresh持有者占比
                Double freshShare = rs.getDouble("fresh_holder_value_share");
                holderStats.setFreshHolderSharePercent(freshShare != null ? freshShare * 100 : 0.0);
                
                // 设置集中度详细信息
                TokenDistribution.Concentration concentration = new TokenDistribution.Concentration();
                concentration.setTop2SharePercent(top2Share != null ? top2Share : 0.0);
                concentration.setGiniCoefficient(concentrationIndex != null ? concentrationIndex : 0.5);
                holderStats.setConcentration(concentration);
                
                return holderStats;
            }, tokenId);
            
        } catch (Exception e) {
            log.error("获取持有者统计失败, tokenId: {}", tokenId, e);
            return new TokenDistribution.HolderStats();
        }
    }
    
    /**
     * 获取标签分布
     */
    private List<TokenDistribution.TagDistribution> getTagDistribution(Long tokenId, String timeRange) {
        try {
            String sql = """
                SELECT
                    tag,
                    value_usd,
                    holders_count,
                    pct_change_1min
                FROM v_token_holder_tag_minute
                WHERE token_id = ?
                ORDER BY value_usd DESC
                LIMIT 5
                """;
            
            List<TokenDistribution.TagDistribution> tagDistList = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenDistribution.TagDistribution tagDist = new TokenDistribution.TagDistribution();
                
                tagDist.setTag(rs.getString("tag"));
                
                BigDecimal valueUsd = rs.getBigDecimal("value_usd");
                tagDist.setValueUsd(valueUsd != null ? valueUsd.toString() : "0");
                
                tagDist.setHoldersCount(rs.getLong("holders_count"));
                
                // 使用数据库中的变化率
                Double changePercent = rs.getDouble("pct_change_1min");
                tagDist.setChange5min(String.format("%+.1f", changePercent != null ? changePercent : 0.0));
                
                // 根据变化百分比计算变化金额
                BigDecimal changeAmount = valueUsd != null && changePercent != null ? 
                    valueUsd.multiply(new BigDecimal(changePercent).divide(new BigDecimal("100"), 4, RoundingMode.HALF_UP)) : 
                    BigDecimal.ZERO;
                tagDist.setChangeAmount5min(changeAmount.toString());
                
                // 设置余额（简单设置为value的80%）
                BigDecimal balance = valueUsd != null ? 
                    valueUsd.multiply(new BigDecimal("0.8")) : 
                    BigDecimal.ZERO;
                tagDist.setBalance(balance.toString());
                
                return tagDist;
            }, tokenId);
            
            // 计算各标签的占比
            BigDecimal totalValue = tagDistList.stream()
                .map(t -> new BigDecimal(t.getValueUsd()))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
            
            for (TokenDistribution.TagDistribution tagDist : tagDistList) {
                if (totalValue.compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal sharePercent = new BigDecimal(tagDist.getValueUsd())
                        .divide(totalValue, 4, RoundingMode.HALF_UP)
                        .multiply(new BigDecimal("100"));
                    tagDist.setSharePercent(sharePercent.doubleValue());
                } else {
                    tagDist.setSharePercent(0.0);
                }
            }
            
            return tagDistList;
            
        } catch (Exception e) {
            log.error("获取标签分布失败, tokenId: {}, timeRange: {}", tokenId, timeRange, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取Top持币地址
     */
    private List<TokenDistribution.TopHolder> getTopHolders(Long tokenId) {
        try {
            String sql = """
                SELECT 
                    account_id,
                    account_address,
                    value_usd,
                    ownership_pct,
                    amount,
                    label_mask
                FROM v_token_top_holders_latest
                WHERE token_id = ?
                ORDER BY value_usd DESC
                LIMIT 10
                """;
            
            return clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenDistribution.TopHolder topHolder = new TokenDistribution.TopHolder();
                
                topHolder.setRank(rowNum + 1);
                topHolder.setAccountId(rs.getString("account_id"));

                topHolder.setAddress(rs.getString("account_address"));
                
                // 根据label_mask生成标签
                int labelMask = rs.getInt("label_mask");
                topHolder.setLabels(generateLabelsFromMask(labelMask));
                
                BigDecimal amount = rs.getBigDecimal("amount");
                topHolder.setBalance(amount != null ? amount.toString() : "0");
                
                BigDecimal valueUsd = rs.getBigDecimal("value_usd");
                topHolder.setValueUsd(valueUsd != null ? valueUsd.toString() : "0");
                
                Double ownershipPct = rs.getDouble("ownership_pct");
                topHolder.setOwnershipPercent(ownershipPct != null ? ownershipPct * 100 : 0.0);
                
                // 生成随机的首次出现天数
                topHolder.setFirstSeenDays((int)(30 + Math.random() * 1000));
                
                return topHolder;
            }, tokenId);
            
        } catch (Exception e) {
            log.error("获取Top持币地址失败, tokenId: {}", tokenId, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 根据label_mask生成标签列表
     */
    private List<String> generateLabelsFromMask(int labelMask) {
        List<String> labels = new ArrayList<>();
        
        if ((labelMask & 1) != 0) labels.add("fresh");
        if ((labelMask & 2) != 0) labels.add("whale");
        if ((labelMask & 4) != 0) labels.add("smart");
        if ((labelMask & 8) != 0) labels.add("cex");
        
        // 如果没有标签，默认给一个
        if (labels.isEmpty()) {
            labels.add("public");
        }
        
        return labels;
    }
    
    @Override
    public TokenPnL findTokenPnL(Long tokenId, String timeRange, Integer topLimit) {
        log.info("获取代币PnL分析数据, tokenId: {}, timeRange: {}, topLimit: {}", tokenId, timeRange, topLimit);
        
        try {
            TokenPnL tokenPnL = new TokenPnL();
            tokenPnL.setTokenId(tokenId);
            tokenPnL.setTimeRange(timeRange);
            tokenPnL.setTopLimit(topLimit);
            
            // 1. 获取Top PnL排行榜
            List<TokenPnL.TopPnLItem> topPnLList = getTopPnLRanking(tokenId, timeRange, topLimit);
            tokenPnL.setTopPnL(topPnLList);
            
            // 2. 获取宏观指标数据
            TokenPnL.Indicators indicators = getMacroIndicators(tokenId);
            tokenPnL.setIndicators(indicators);
            
            // 3. 获取汇总统计
            TokenPnL.Summary summary = calculatePnLSummary(tokenId, timeRange);
            tokenPnL.setSummary(summary);
            
            // 4. 设置宏观PnL数据
            TokenPnL.MacroPnL macroPnL = new TokenPnL.MacroPnL();
            macroPnL.setLastUpdated(java.time.Instant.now().toString());
            tokenPnL.setMacroPnL(macroPnL);
            
            return tokenPnL;
            
        } catch (Exception e) {
            log.error("获取代币PnL分析数据失败, tokenId: {}", tokenId, e);
            return null;
        }
    }
    
    /**
     * 获取Top PnL排行榜
     */
    private List<TokenPnL.TopPnLItem> getTopPnLRanking(Long tokenId, String timeRange, Integer topLimit) {
        try {
            // 获取时间过滤条件
            String timeCondition = resolveTimeWindow(timeRange);
            
            String sql = """
                SELECT 
                    account_id,
                    
                    total_pnl_usd,
                    realized_pnl_usd,
                    unrealized_pnl_usd,
                    roi_pct,
                    holding_pct,
                    account_address
                FROM ch_account_pnl_current_ma
                WHERE token_id = ?
                  AND position > 0
                  AND last_tx_time >= now() - INTERVAL 1 DAY
                ORDER BY total_pnl_usd DESC
                LIMIT ?
                """;
            
            return clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenPnL.TopPnLItem item = new TokenPnL.TopPnLItem();
                
                item.setAccountId(rs.getLong("account_id"));
                item.setAddress(rs.getString("account_address"));
                item.setTotalPnlUsd(rs.getString("total_pnl_usd"));
                item.setRealizedPnlUsd(rs.getString("realized_pnl_usd"));
                item.setUnrealizedPnlUsd(rs.getString("unrealized_pnl_usd"));
                item.setTotalRoi(rs.getDouble("roi_pct"));
                item.setStillHoldingPercent(rs.getDouble("holding_pct"));
                
                // 生成随机标签
                item.setLabels(generateLabels(rs.getLong("account_id")));
                
                return item;
            }, tokenId, topLimit);
            
        } catch (Exception e) {
            log.error("获取Top PnL排行榜失败, tokenId: {}", tokenId, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取宏观指标数据
     */
    private TokenPnL.Indicators getMacroIndicators(Long tokenId) {
        try {
            String sql = """
                SELECT 
                    nupl,
                    mvrv, 
                    sopr
                FROM v_token_macro_latest
                WHERE token_id = ?
                """;
            
            List<TokenPnL.Indicators> results = clickhouseJdbcTemplate.query(sql, (rs, rowNum) -> {
                TokenPnL.Indicators indicators = new TokenPnL.Indicators();
                
                log.info("获取宏观指标原始数据 - tokenId: {}, nupl: {}, mvrv: {}, sopr: {}", 
                    tokenId, rs.getDouble("nupl"), rs.getDouble("mvrv"), rs.getDouble("sopr"));
                
                // NUPL指标
                Double nupl = rs.getDouble("nupl");
                if (!rs.wasNull() && nupl != null) {
                    String nuplInterpretation = interpretNUPL(nupl);
                    indicators.setNUPL(new TokenPnL.IndicatorValue(nupl, "网络未实现损益", nuplInterpretation));
                    log.info("设置NUPL指标: {}", nupl);
                }
                
                // MVRV指标
                Double mvrv = rs.getDouble("mvrv");
                if (!rs.wasNull() && mvrv != null) {
                    String mvrvInterpretation = interpretMVRV(mvrv);
                    indicators.setMVRV(new TokenPnL.IndicatorValue(mvrv, "市值实现价值比", mvrvInterpretation));
                    log.info("设置MVRV指标: {}", mvrv);
                }
                
                // SOPR指标
                Double sopr = rs.getDouble("sopr");
                if (!rs.wasNull() && sopr != null) {
                    String soprInterpretation = interpretSOPR(sopr);
                    indicators.setSOPR(new TokenPnL.IndicatorValue(sopr, "已用输出利润率", soprInterpretation));
                    log.info("设置SOPR指标: {}", sopr);
                }
                
                return indicators;
            }, tokenId);
            
            if (results.isEmpty()) {
                log.warn("未找到tokenId: {} 的宏观指标数据", tokenId);
                return new TokenPnL.Indicators();
            }
            
            return results.get(0);
            
        } catch (Exception e) {
            log.error("获取宏观指标数据失败, tokenId: {}", tokenId, e);
            return new TokenPnL.Indicators();
        }
    }
    
    /**
     * 计算PnL汇总统计
     */
    private TokenPnL.Summary calculatePnLSummary(Long tokenId, String timeRange) {
        try {
            String timeCondition = resolveTimeWindow(timeRange);
            
            String sql = """
                SELECT 
                    sum(total_pnl_usd) AS total_pnl,
                    sum(realized_pnl_usd) AS total_realized_pnl,
                    sum(unrealized_pnl_usd) AS total_unrealized_pnl,
                    count(*) AS total_accounts,
                    sum(CASE WHEN total_pnl_usd > 0 THEN 1 ELSE 0 END) AS profitable_count,
                    avg(holding_pct) AS avg_still_holding_percent
                FROM ch_account_pnl_current_ma
                WHERE token_id = ?
                  AND position > 0
                  AND last_tx_time >= now() - INTERVAL 1 DAY
                """;
            
            return clickhouseJdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                TokenPnL.Summary summary = new TokenPnL.Summary();
                
                summary.setTotalPnL(rs.getString("total_pnl"));
                summary.setTotalRealizedPnL(rs.getString("total_realized_pnl"));
                summary.setTotalUnrealizedPnL(rs.getString("total_unrealized_pnl"));
                
                int totalAccounts = rs.getInt("total_accounts");
                int profitableCount = rs.getInt("profitable_count");
                
                summary.setProfitableCount(profitableCount);
                summary.setProfitablePercentage(totalAccounts > 0 ? (double) profitableCount / totalAccounts * 100 : 0.0);
                summary.setAvgStillHoldingPercent(rs.getDouble("avg_still_holding_percent"));
                
                return summary;
            }, tokenId);
            
        } catch (Exception e) {
            log.error("计算PnL汇总统计失败, tokenId: {}", tokenId, e);
            return new TokenPnL.Summary();
        }
    }
    
    /**
     * 解释NUPL指标
     */
    private String interpretNUPL(Double nupl) {
        if (nupl == null) return "数据不足";
        if (nupl > 0.75) return "极度贪婪区间";
        if (nupl > 0.5) return "贪婪区间";
        if (nupl > 0.25) return "乐观区间";
        if (nupl > 0) return "希望区间";
        if (nupl > -0.25) return "恐惧区间";
        if (nupl > -0.5) return "焦虑区间";
        return "投降区间";
    }
    
    /**
     * 解释MVRV指标
     */
    private String interpretMVRV(Double mvrv) {
        if (mvrv == null) return "数据不足";
        if (mvrv > 3.5) return "价格被高估";
        if (mvrv > 2.5) return "价格偏高";
        if (mvrv > 1.5) return "价格高于历史平均成本";
        if (mvrv > 0.8) return "价格合理";
        return "价格被低估";
    }
    
    /**
     * 解释SOPR指标
     */
    private String interpretSOPR(Double sopr) {
        if (sopr == null) return "数据不足";
        if (sopr > 1.05) return "市场整体盈利";
        if (sopr > 0.95) return "市场基本平衡";
        return "市场整体亏损";
    }
}
