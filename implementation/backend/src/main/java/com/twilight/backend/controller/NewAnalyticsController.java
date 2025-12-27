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
}