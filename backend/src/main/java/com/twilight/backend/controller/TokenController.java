package com.twilight.backend.controller;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.TokenInfo;
import com.twilight.backend.model.TokenMetrics;
import com.twilight.backend.model.TokenPriceHistory;
import com.twilight.backend.service.TokenService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 代币相关接口控制器
 */
@Slf4j
@RestController
@RequestMapping("/v1/tokens")
@RequiredArgsConstructor
@Tag(name = "代币接口", description = "代币相关的API接口")
public class TokenController {

    private final TokenService tokenService;

    /**
     * 获取代币基础信息
     */
    @GetMapping("/{tokenId}/info")
    @Operation(summary = "获取代币基础信息", description = "根据代币ID获取代币的基础信息，包括名称、符号、年龄、安全评分等")
    public ApiResponse<TokenInfo> getTokenInfo(
            @Parameter(description = "代币ID", example = "1")
            @PathVariable Long tokenId) {
        
        log.info("获取代币信息请求, tokenId: {}", tokenId);
        
        TokenInfo tokenInfo = tokenService.getTokenInfo(tokenId);
        if (tokenInfo == null) {
            return ApiResponse.badRequest("代币不存在");
        }
        
        return ApiResponse.success(tokenInfo);
    }

    /**
     * 获取所有代币列表
     */
    @GetMapping("/list")
    @Operation(summary = "获取代币列表", description = "获取系统中所有代币的基础信息列表")
    public ApiResponse<List<TokenInfo>> getTokenList() {
        
        log.info("获取代币列表请求");
        
        List<TokenInfo> tokens = tokenService.getAllTokens();
        
        return ApiResponse.success(tokens);
    }

    /**
     * 获取代币宏观指标
     */
    @GetMapping("/{tokenId}/metrics")
    @Operation(summary = "获取代币宏观指标", description = "获取代币的当前价格、市值、FDV、流动性等宏观指标")
    public ApiResponse<TokenMetrics> getTokenMetrics(
            @Parameter(description = "代币ID", example = "1")
            @PathVariable Long tokenId) {
        
        log.info("获取代币宏观指标请求, tokenId: {}", tokenId);
        
        TokenMetrics metrics = tokenService.getTokenMetrics(tokenId);
        if (metrics == null) {
            return ApiResponse.badRequest("代币指标不存在");
        }
        
        return ApiResponse.success(metrics);
    }

    /**
     * 获取代币历史价格
     */
    @GetMapping("/{tokenId}/price-history")
    @Operation(summary = "获取代币历史价格", description = "获取代币在指定时间范围内的历史价格数据")
    public ApiResponse<List<TokenPriceHistory>> getPriceHistory(
            @Parameter(description = "代币ID", example = "1")
            @PathVariable Long tokenId,
            @Parameter(description = "时间范围", example = "24h", 
                      schema = @io.swagger.v3.oas.annotations.media.Schema(
                          allowableValues = {"1h", "24h", "7d", "30d"}))
            @RequestParam(defaultValue = "24h") String timeRange) {
        
        log.info("获取代币历史价格请求, tokenId: {}, timeRange: {}", tokenId, timeRange);
        
        List<TokenPriceHistory> history = tokenService.getPriceHistory(tokenId, timeRange);
        
        return ApiResponse.success(history);
    }
}
