package com.twilight.backend.controller;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.PerpContextMetric;
import com.twilight.backend.model.PerpExecutionMetric;
import com.twilight.backend.model.PerpPanelMetric;
import com.twilight.backend.model.PerpSignal;
import com.twilight.backend.repository.PerpRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 永续合约指标API
 */
@Slf4j
@RestController
@RequestMapping("/v1/perps")
@RequiredArgsConstructor
@Tag(name = "Perp Analytics API", description = "永续合约执行/语境/面板及信号查询接口")
public class PerpAnalyticsController {

    private final PerpRepository perpRepository;

    @GetMapping("/markets")
    @Operation(summary = "获取永续市场快照",
            description = "返回最新一分钟的合约面板指标，用于大盘展示与排序")
    public ApiResponse<List<PerpPanelMetric>> getMarketSnapshots(
            @RequestParam(required = false) List<String> symbols,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, name = "algo") String algoVersion,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "20") Integer pageSize,
            @RequestParam(defaultValue = "volume") String sortBy,
            @RequestParam(defaultValue = "desc") String order) {

        List<String> normalizedSymbols = normalizeQueryList(symbols);

        try {
            List<PerpPanelMetric> snapshots = perpRepository.findLatestPanelSnapshots(
                    normalizedSymbols,
                    exchange,
                    algoVersion,
                    page != null ? page : 1,
                    pageSize != null ? pageSize : 20,
                    sortBy,
                    order);
            return ApiResponse.success(snapshots);
        } catch (Exception ex) {
            log.error("获取永续市场快照失败 symbols={}, exchange={}, algo={}",
                    normalizedSymbols, exchange, algoVersion, ex);
            return ApiResponse.serverError("获取永续市场快照失败: " + ex.getMessage());
        }
    }

    @GetMapping("/{symbol}/execution")
    @Operation(summary = "获取永续执行面时间序列",
            description = "查询秒级执行面指标，用于盘口&交易健康监控")
    public ApiResponse<List<PerpExecutionMetric>> getExecutionSeries(
            @PathVariable String symbol,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, name = "algo") String algoVersion,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "1800") Integer limit) {

        try {
            List<PerpExecutionMetric> metrics = perpRepository.findExecutionMetrics(
                    symbol,
                    exchange,
                    algoVersion,
                    startTime,
                    endTime,
                    limit != null ? limit : 1800);
            return ApiResponse.success(metrics);
        } catch (Exception ex) {
            log.error("获取执行面指标失败 symbol={}, exchange={}, algo={}",
                    symbol, exchange, algoVersion, ex);
            return ApiResponse.serverError("获取执行面指标失败: " + ex.getMessage());
        }
    }

    @GetMapping("/{symbol}/context")
    @Operation(summary = "获取永续语境面时间序列",
            description = "查询分钟级语境指标，包含资金费率、持仓量等")
    public ApiResponse<List<PerpContextMetric>> getContextSeries(
            @PathVariable String symbol,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, name = "algo") String algoVersion,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "1440") Integer limit) {

        try {
            List<PerpContextMetric> metrics = perpRepository.findContextMetrics(
                    symbol,
                    exchange,
                    algoVersion,
                    startTime,
                    endTime,
                    limit != null ? limit : 1440);
            return ApiResponse.success(metrics);
        } catch (Exception ex) {
            log.error("获取语境面指标失败 symbol={}, exchange={}, algo={}",
                    symbol, exchange, algoVersion, ex);
            return ApiResponse.serverError("获取语境面指标失败: " + ex.getMessage());
        }
    }

    @GetMapping("/{symbol}/panel")
    @Operation(summary = "获取永续面板时间序列",
            description = "查询分钟级面板指标（执行+语境+衍生得分）")
    public ApiResponse<List<PerpPanelMetric>> getPanelSeries(
            @PathVariable String symbol,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, name = "algo") String algoVersion,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "1440") Integer limit) {

        try {
            List<PerpPanelMetric> metrics = perpRepository.findPanelMetrics(
                    symbol,
                    exchange,
                    algoVersion,
                    startTime,
                    endTime,
                    limit != null ? limit : 1440);
            return ApiResponse.success(metrics);
        } catch (Exception ex) {
            log.error("获取面板指标失败 symbol={}, exchange={}, algo={}",
                    symbol, exchange, algoVersion, ex);
            return ApiResponse.serverError("获取面板指标失败: " + ex.getMessage());
        }
    }

    @GetMapping("/signals")
    @Operation(summary = "获取异常信号",
            description = "返回异常信号列表，支持交易对/交易所/类型/等级筛选")
    public ApiResponse<List<PerpSignal>> getSignals(
            @RequestParam(required = false) List<String> symbols,
            @RequestParam(required = false) List<String> exchanges,
            @RequestParam(required = false) List<String> types,
            @RequestParam(required = false) List<String> levels,
            @RequestParam(required = false, name = "algo") String algoVersion,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "200") Integer limit) {

        List<String> normalizedSymbols = normalizeQueryList(symbols);
        List<String> normalizedExchanges = normalizeQueryList(exchanges);
        List<String> normalizedTypes = normalizeQueryList(types);
        List<String> normalizedLevels = normalizeQueryList(levels);

        try {
            List<PerpSignal> signals = perpRepository.findSignals(
                    normalizedSymbols,
                    normalizedExchanges,
                    normalizedTypes,
                    normalizedLevels,
                    algoVersion,
                    startTime,
                    endTime,
                    limit != null ? limit : 200);
            return ApiResponse.success(signals);
        } catch (Exception ex) {
            log.error("获取永续信号失败 symbols={}, exchanges={}, types={}",
                    normalizedSymbols, normalizedExchanges, normalizedTypes, ex);
            return ApiResponse.serverError("获取永续信号失败: " + ex.getMessage());
        }
    }

    private List<String> normalizeQueryList(List<String> rawValues) {
        if (rawValues == null || rawValues.isEmpty()) {
            return null;
        }

        List<String> normalized = rawValues.stream()
                .flatMap(value -> Arrays.stream(value.split(",")))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());

        return normalized.isEmpty() ? null : normalized;
    }
}
