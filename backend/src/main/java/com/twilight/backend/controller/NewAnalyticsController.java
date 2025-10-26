package com.twilight.backend.controller;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.TokenListItem;
import com.twilight.backend.model.TokenOverview;
import com.twilight.backend.model.TokenDistribution;
import com.twilight.backend.model.AccountDetail;

import com.twilight.backend.repository.*;
import com.twilight.backend.service.WebSocketPushService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.*;
import com.twilight.backend.repository.AccountRepository;
import com.twilight.backend.repository.TokenRepository;
import com.twilight.backend.model.TokenPnL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NewAnalytics前端专用API控制器
 * 按照前端V2.0规范设计的合并接口
 */
@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Tag(name = "NewAnalytics API", description = "前端NewAnalytics组件专用接口")
public class NewAnalyticsController {

    private final TokenRepository tokenRepository;
    private final AccountRepository accountRepository;
    private final WebSocketPushService webSocketPushService;

    // private final TokenOverviewService tokenOverviewService;
    // private final EnhancedWebSocketService webSocketService;
    // private final PnLRepository pnLRepository;
    // private final DistributionRepository distributionRepository;
    // private final AccountRepository accountRepository;

    // ============ REST API 接口 ============

    /**
     * 1. 代币列表接口
     */
    @GetMapping("/health")
    public ApiResponse<String> health() {
        return ApiResponse.success("OK");
    }

    @GetMapping("/tokens/list")
    @Operation(summary = "获取代币列表", description = "获取代币列表，包含基础信息和关键指标")
    public ApiResponse<List<TokenListItem>> getTokenList(
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "50") Integer pageSize,
            @RequestParam(defaultValue = "mcap") String sortBy,
            @RequestParam(defaultValue = "desc") String order) {
        
        log.info("获取代币列表, page: {}, pageSize: {}, sortBy: {}, order: {}", 
            page, pageSize, sortBy, order);
        
        try {
            // 获取代币列表数据
            List<TokenListItem> tokenItems = tokenRepository.findTokenListItems(page, pageSize, sortBy, order);
            
            log.info("成功获取代币列表, 返回{}条记录", tokenItems.size());
            return ApiResponse.success(tokenItems);
            
        } catch (Exception e) {
            log.error("获取代币列表失败", e);
            return ApiResponse.serverError("获取代币列表失败: " + e.getMessage());
        }
    }

    /**
     * 2. 代币大盘&交易流合并接口 - 核心接口
     */
    @GetMapping("/tokens/{tokenId}/overview")
    @Operation(summary = "获取代币完整概览", 
               description = "合并接口：基础信息+宏观指标+交易流分析")
    public ApiResponse<TokenOverview> getTokenOverview(
            @Parameter(description = "代币ID", example = "1")
            @PathVariable Long tokenId,
            @Parameter(description = "时间窗口：20s/1min/5min/1h", example = "5min")
            @RequestParam(defaultValue = "5min") String timeRange) {
        
        log.info("获取代币完整概览, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        try {
            TokenOverview overview = tokenRepository.findTokenOverview(tokenId, timeRange);
            if (overview == null) {
                return ApiResponse.badRequest("代币不存在或数据不足");
            }
            return ApiResponse.success(overview);
        } catch (Exception e) {
            log.error("获取代币概览失败", e);
            return ApiResponse.serverError("获取代币概览失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取代币分布分析
     */
    @GetMapping("/tokens/{tokenId}/distribution")
    @Operation(summary = "获取代币分布", description = "获取代币的持有者分布分析，包括持有者统计、标签分布、Top持币地址等")
    public ApiResponse<TokenDistribution> getTokenDistribution(
            @Parameter(description = "代币ID") @PathVariable Long tokenId,
            @Parameter(description = "时间范围") @RequestParam(defaultValue = "5min") String timeRange) {
        
        try {
            TokenDistribution distribution = tokenRepository.findTokenDistribution(tokenId, timeRange);
            return ApiResponse.success(distribution);
        } catch (Exception e) {
            log.error("获取代币分布失败: {}", e.getMessage(), e);
            return ApiResponse.serverError("获取代币分布失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取账户详情
     */
    @GetMapping("/accounts/{accountId}")
    @Operation(summary = "获取账户详情", description = "获取账户的基础信息、资产持仓、转账历史等详细信息")
    public ApiResponse<AccountDetail> getAccountDetail(
            @Parameter(description = "账户ID") @PathVariable Long accountId) {
        
        try {
            AccountDetail accountDetail = accountRepository.findAccountDetailById(accountId);
            if (accountDetail == null) {
                return ApiResponse.badRequest("账户不存在");
            }
            return ApiResponse.success(accountDetail);
        } catch (Exception e) {
            log.error("获取账户详情失败: {}", e.getMessage(), e);
            return ApiResponse.serverError("获取账户详情失败: " + e.getMessage());
        }
    }

    /**
     * 获取代币PnL分析数据
     */
    @GetMapping("/tokens/{tokenId}/pnl")
    @Operation(summary = "获取代币PnL分析", description = "获取代币的PnL排行榜、宏观指标和汇总统计")
    public ApiResponse<TokenPnL> getTokenPnL(
            @Parameter(description = "代币ID") @PathVariable Long tokenId,
            @Parameter(description = "时间范围") @RequestParam(defaultValue = "1min") String timeRange,
            @Parameter(description = "Top PnL排行榜数量限制") @RequestParam(defaultValue = "50") Integer topLimit) {
        
        log.info("获取代币PnL分析, tokenId: {}, timeRange: {}, topLimit: {}", tokenId, timeRange, topLimit);
        
        try {
            TokenPnL tokenPnL = tokenRepository.findTokenPnL(tokenId, timeRange, topLimit);
            
            if (tokenPnL == null) {
                return ApiResponse.error(404, "代币PnL数据不存在");
            }
            
            return ApiResponse.success(tokenPnL);
            
        } catch (Exception e) {
            log.error("获取代币PnL分析失败", e);
            return ApiResponse.serverError("获取代币PnL分析失败: " + e.getMessage());
        }
    }

//     /**
//      * 3. PnL分析接口 (旧版本，已被上面的新接口替换)
//      */
//     @GetMapping("/tokens/{tokenId}/overview")
//     @Operation(summary = "获取代币完整概览", 
//                description = "合并接口：基础信息+宏观指标+价格走势+交易流分析")
//     public ApiResponse<Map<String, Object>> getTokenOverview(
//             @Parameter(description = "代币ID", example = "1")
//             @PathVariable Long tokenId,
//             @Parameter(description = "时间窗口：20s/1min/5min/1h", example = "5min")
//             @RequestParam(defaultValue = "5min") String timeRange) {
        
//         log.info("获取代币完整概览, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
//         try {
//             Map<String, Object> overview = tokenOverviewService.getTokenOverview(tokenId, timeRange);
//             return ApiResponse.success(overview);
//         } catch (Exception e) {
//             log.error("获取代币概览失败", e);
//             return ApiResponse.error("获取代币概览失败: " + e.getMessage());
//         }
//     }

//     /**
//      * 3. PnL分析接口
//      */
//     @GetMapping("/tokens/{tokenId}/pnl")
//     @Operation(summary = "获取PnL分析数据", description = "获取Top PnL、分布统计、宏观指标")
//     public ApiResponse<Map<String, Object>> getTokenPnL(
//             @PathVariable Long tokenId,
//             @RequestParam(defaultValue = "5min") String timeRange,
//             @RequestParam(defaultValue = "50") Integer limit) {
        
//         log.info("获取PnL分析数据, tokenId: {}, timeRange: {}, limit: {}", tokenId, timeRange, limit);
        
//         Map<String, Object> pnlData = new HashMap<>();
//         pnlData.put("tokenId", tokenId);
//         pnlData.put("timeRange", timeRange);
        
//         try {
//             // 获取Top PnL
//             var topPnLList = pnLRepository.findTopPnL(tokenId, limit);
//             pnlData.put("topPnL", buildTopPnLForFrontend(topPnLList));
            
//             // 获取分布统计
//             pnlData.put("distribution", buildPnLDistribution(topPnLList));
            
//             // 获取宏观指标
//             var macroPnL = pnLRepository.findLatestMacroPnL(tokenId);
//             pnlData.put("macroIndicators", buildMacroIndicators(macroPnL));
            
//         } catch (Exception e) {
//             log.error("获取PnL数据失败", e);
//             pnlData.put("error", "数据获取失败: " + e.getMessage());
//         }
        
//         return ApiResponse.success(pnlData);
//     }

//     /**
//      * 4. 分布分析接口
//      */
//     @GetMapping("/tokens/{tokenId}/distribution")
//     @Operation(summary = "获取分布分析数据", description = "获取持有者统计、标签分布、Top持币地址")
//     public ApiResponse<Map<String, Object>> getTokenDistribution(
//             @PathVariable Long tokenId,
//             @RequestParam(defaultValue = "5min") String timeRange) {
        
//         log.info("获取分布分析数据, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
//         Map<String, Object> distributionData = new HashMap<>();
//         distributionData.put("tokenId", tokenId);
//         distributionData.put("timeRange", timeRange);
        
//         try {
//             // 获取最新分布数据
//             var distribution = distributionRepository.findLatestDistribution(tokenId);
//             if (distribution != null) {
//                 distributionData.put("holderStats", buildHolderStats(distribution));
//             }
            
//             // 获取标签分布
//             var tagHoldings = distributionRepository.findTagHoldings(tokenId, java.time.LocalDateTime.now());
//             distributionData.put("tagDistribution", buildTagDistribution(tagHoldings));
            
//             // 获取Top持币地址
//             var topHolders = distributionRepository.findTopHolders(tokenId, 100);
//             distributionData.put("topHolders", buildTopHolders(topHolders));
            
//         } catch (Exception e) {
//             log.error("获取分布数据失败", e);
//             distributionData.put("error", "数据获取失败: " + e.getMessage());
//         }
        
//         return ApiResponse.success(distributionData);
//     }

//     /**
//      * 5. 账户列表接口
//      */
//     @GetMapping("/accounts")
//     @Operation(summary = "获取账户列表", description = "获取账户列表，支持标签筛选")
//     public ApiResponse<Map<String, Object>> getAccountList(
//             @RequestParam(defaultValue = "1") Integer page,
//             @RequestParam(defaultValue = "20") Integer pageSize,
//             @RequestParam(required = false) String[] labels,
//             @RequestParam(defaultValue = "balance") String sortBy) {
        
//         log.info("获取账户列表, page: {}, pageSize: {}, labels: {}, sortBy: {}", 
//             page, pageSize, labels, sortBy);
        
//         Map<String, Object> result = new HashMap<>();
        
//         try {
//             int offset = (page - 1) * pageSize;
//             var accounts = accountRepository.findAllAccounts(offset, pageSize);
//             var total = accountRepository.countAccounts();
            
//             result.put("data", buildAccountListForFrontend(accounts));
//             result.put("total", total);
//             result.put("page", page);
//             result.put("pageSize", pageSize);
            
//         } catch (Exception e) {
//             log.error("获取账户列表失败", e);
//             result.put("error", "数据获取失败: " + e.getMessage());
//         }
        
//         return ApiResponse.success(result);
//     }

    // ============ WebSocket 消息处理 ============

    @MessageMapping("/analytics/tokens/list")
    @SendTo("/topic/analytics/tokens/list")
    public ApiResponse<List<TokenListItem>> streamTokenList(TokenListSubscriptionRequest request) {
        TokenListSubscriptionRequest actual = request != null ? request : new TokenListSubscriptionRequest();
        log.info("WebSocket订阅代币列表, page: {}, pageSize: {}, sortBy: {}, order: {}",
                actual.getPage(), actual.getPageSize(), actual.getSortBy(), actual.getOrder());
        try {
            List<TokenListItem> tokenItems = tokenRepository.findTokenListItems(
                    actual.getPage(), actual.getPageSize(), actual.getSortBy(), actual.getOrder());
            log.info("WebSocket推送代币列表, 返回{}条记录", tokenItems.size());
            return ApiResponse.success(tokenItems);
        } catch (Exception e) {
            log.error("WebSocket推送代币列表失败", e);
            return ApiResponse.serverError("获取代币列表失败: " + e.getMessage());
        }
    }

    @MessageMapping("/analytics/tokens/{tokenId}/overview")
    public void streamTokenOverview(@DestinationVariable Long tokenId,
                                    TokenTimeRangeRequest request) {
        TokenTimeRangeRequest actual = request != null ? request : TokenTimeRangeRequest.defaultRequest();
        log.info("WebSocket注册代币概览订阅, tokenId: {}, timeRange: {}", tokenId, actual.getTimeRange());
        
        // 注册订阅，后续由定时任务推送数据
        webSocketPushService.registerOverviewSubscription(tokenId, actual.getTimeRange());
    }

    @MessageMapping("/analytics/tokens/{tokenId}/distribution")
    public void streamTokenDistribution(@DestinationVariable Long tokenId,
                                        TokenTimeRangeRequest request) {
        TokenTimeRangeRequest actual = request != null ? request : TokenTimeRangeRequest.defaultRequest();
        log.info("WebSocket注册代币分布订阅, tokenId: {}, timeRange: {}", tokenId, actual.getTimeRange());
        
        // 注册订阅，后续由定时任务推送数据
        webSocketPushService.registerDistributionSubscription(tokenId, actual.getTimeRange());
    }

    @MessageMapping("/analytics/accounts/{accountId}")
    @SendTo("/topic/analytics/accounts/{accountId}")
    public ApiResponse<AccountDetail> streamAccountDetail(@DestinationVariable Long accountId) {
        log.info("WebSocket订阅账户详情, accountId: {}", accountId);
        try {
            AccountDetail accountDetail = accountRepository.findAccountDetailById(accountId);
            if (accountDetail == null) {
                return ApiResponse.badRequest("账户不存在");
            }
            return ApiResponse.success(accountDetail);
        } catch (Exception e) {
            log.error("WebSocket推送账户详情失败", e);
            return ApiResponse.serverError("获取账户详情失败: " + e.getMessage());
        }
    }

    @MessageMapping("/analytics/tokens/{tokenId}/pnl")
    public void streamTokenPnL(@DestinationVariable Long tokenId,
                               TokenPnLSubscriptionRequest request) {
        TokenPnLSubscriptionRequest actual = request != null ? request : new TokenPnLSubscriptionRequest();
        log.info("WebSocket注册代币PnL订阅, tokenId: {}, timeRange: {}, topLimit: {}",
                tokenId, actual.getTimeRange(), actual.getTopLimit());
        
        // 注册订阅，后续由定时任务推送数据
        webSocketPushService.registerPnLSubscription(tokenId, actual.getTimeRange(), actual.getTopLimit());
    }

    @Data
    private static class TokenListSubscriptionRequest {
        private Integer page = 1;
        private Integer pageSize = 50;
        private String sortBy = "mcap";
        private String order = "desc";
    }

    @Data
    private static class TokenTimeRangeRequest {
        private String timeRange = "5min";

        static TokenTimeRangeRequest defaultRequest() {
            return new TokenTimeRangeRequest();
        }
    }

    @Data
    private static class TokenPnLSubscriptionRequest {
        private String timeRange = "1min";
        private Integer topLimit = 50;
    }

//     // ============ 数据构建辅助方法 ============

//     private java.util.List<Map<String, Object>> buildTopPnLForFrontend(
//             java.util.List<com.twilight.backend.model.TopPnLInfo> topPnLList) {
//         return topPnLList.stream()
//             .map(pnl -> {
//                 Map<String, Object> item = new HashMap<>();
//                 item.put("rank", topPnLList.indexOf(pnl) + 1);
//                 item.put("address", pnl.getAccountAddress());
//                 item.put("labels", pnl.getLabels() != null ? pnl.getLabels() : java.util.List.of());
//                 item.put("totalPnlUsd", pnl.getTotalPnlUsd() != null ? pnl.getTotalPnlUsd().toString() : "0");
//                 item.put("realizedPnlUsd", pnl.getRealizedPnlUsd() != null ? pnl.getRealizedPnlUsd().toString() : "0");
//                 item.put("unrealizedPnlUsd", pnl.getUnrealizedPnlUsd() != null ? pnl.getUnrealizedPnlUsd().toString() : "0");
//                 item.put("totalRoi", pnl.getTotalRoi() != null ? pnl.getTotalRoi() : 0.0);
//                 item.put("stillHoldingPercent", pnl.getStillHoldingPercent() != null ? pnl.getStillHoldingPercent() : 0.0);
//                 return item;
//             })
//             .collect(java.util.stream.Collectors.toList());
//     }

//     private Map<String, Object> buildPnLDistribution(java.util.List<com.twilight.backend.model.TopPnLInfo> topPnLList) {
//         long profitable = topPnLList.stream()
//             .mapToLong(pnl -> pnl.getTotalPnlUsd() != null && 
//                 pnl.getTotalPnlUsd().compareTo(java.math.BigDecimal.ZERO) > 0 ? 1 : 0)
//             .sum();
        
//         Map<String, Object> distribution = new HashMap<>();
//         distribution.put("profitable", profitable);
//         distribution.put("unprofitable", topPnLList.size() - profitable);
//         distribution.put("totalAccounts", topPnLList.size());
        
//         var totalPnl = topPnLList.stream()
//             .map(com.twilight.backend.model.TopPnLInfo::getTotalPnlUsd)
//             .filter(java.util.Objects::nonNull)
//             .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add);
//         distribution.put("totalPnlUsd", totalPnl.toString());
        
//         var avgPnl = topPnLList.size() > 0 ? 
//             totalPnl.divide(java.math.BigDecimal.valueOf(topPnLList.size()), 2, java.math.RoundingMode.HALF_UP) : 
//             java.math.BigDecimal.ZERO;
//         distribution.put("avgPnlUsd", avgPnl.toString());
        
//         return distribution;
//     }

//     private java.util.List<Map<String, Object>> buildMacroIndicators(
//             com.twilight.backend.model.MacroPnLMetrics macroPnL) {
//         java.util.List<Map<String, Object>> indicators = new java.util.ArrayList<>();
        
//         if (macroPnL != null) {
//             // NUPL指标
//             Map<String, Object> nupl = new HashMap<>();
//             nupl.put("name", "NUPL");
//             nupl.put("value", macroPnL.getNupl() != null ? macroPnL.getNupl().toString() : "0");
//             nupl.put("change", "+0%"); // TODO: 计算变化
//             nupl.put("description", "Net Unrealized Profit/Loss");
//             indicators.add(nupl);
            
//             // MVRV指标
//             Map<String, Object> mvrv = new HashMap<>();
//             mvrv.put("name", "MVRV");
//             mvrv.put("value", macroPnL.getMvrv() != null ? macroPnL.getMvrv().toString() : "0");
//             mvrv.put("change", "+0%"); // TODO: 计算变化
//             mvrv.put("description", "Market Value to Realized Value");
//             indicators.add(mvrv);
            
//             // SOPR指标
//             Map<String, Object> sopr = new HashMap<>();
//             sopr.put("name", "SOPR");
//             sopr.put("value", macroPnL.getSopr() != null ? macroPnL.getSopr().toString() : "0");
//             sopr.put("change", "+0%"); // TODO: 计算变化
//             sopr.put("description", "Spent Output Profit Ratio");
//             indicators.add(sopr);
//         }
        
//         return indicators;
//     }

//     private Map<String, Object> buildHolderStats(com.twilight.backend.model.TokenDistribution distribution) {
//         Map<String, Object> stats = new HashMap<>();
//         stats.put("totalHolders", distribution.getHoldersCount());
        
//         Map<String, Object> concentration = new HashMap<>();
//         concentration.put("top10", distribution.getTop10SharePercent() / 100.0); // 转换为0-1比例
//         concentration.put("giniCoefficient", 0.68); // TODO: 从数据中获取
        
//         stats.put("concentration", concentration);
//         return stats;
//     }

//     private java.util.List<Map<String, Object>> buildTagDistribution(
//             java.util.List<com.twilight.backend.model.TagHolding> tagHoldings) {
//         return tagHoldings.stream()
//             .map(tag -> {
//                 Map<String, Object> item = new HashMap<>();
//                 item.put("tag", tag.getTag());
//                 item.put("holderCount", tag.getHoldersCount());
//                 item.put("totalBalance", tag.getValueUsd() != null ? tag.getValueUsd().toString() : "0");
//                 item.put("percentage", 0.0); // TODO: 计算百分比
//                 item.put("avgBalance", "0"); // TODO: 计算平均余额
//                 item.put("change24h", "+0%"); // TODO: 计算24h变化
//                 return item;
//             })
//             .collect(java.util.stream.Collectors.toList());
//     }

//     private java.util.List<Map<String, Object>> buildTopHolders(
//             java.util.List<com.twilight.backend.model.TopHolder> topHolders) {
//         return topHolders.stream()
//             .map(holder -> {
//                 Map<String, Object> item = new HashMap<>();
//                 item.put("rank", topHolders.indexOf(holder) + 1);
//                 item.put("address", holder.getAccountAddress());
//                 item.put("labels", holder.getLabels() != null ? holder.getLabels() : java.util.List.of());
//                 item.put("balance", holder.getBalance() != null ? holder.getBalance().toString() : "0");
//                 item.put("percentage", holder.getOwnershipPercent() != null ? holder.getOwnershipPercent() : 0.0);
//                 item.put("firstSeenDays", 0); // TODO: 从数据中获取
//                 return item;
//             })
//             .collect(java.util.stream.Collectors.toList());
//     }

//     private java.util.List<Map<String, Object>> buildAccountListForFrontend(
//             java.util.List<com.twilight.backend.model.AccountInfo> accounts) {
//         return accounts.stream()
//             .map(account -> {
//                 Map<String, Object> item = new HashMap<>();
//                 item.put("address", account.getAddress());
//                 item.put("labels", account.getLabels() != null ? account.getLabels() : java.util.List.of());
//                 item.put("balance", "0"); // TODO: 从资产数据中计算
//                 item.put("value", "0"); // TODO: 从资产数据中计算
//                 item.put("pnl", "0"); // TODO: 从PnL数据中获取
//                 item.put("roi", 0.0); // TODO: 从PnL数据中计算
//                 return item;
//             })
//             .collect(java.util.stream.Collectors.toList());
//     }
}
