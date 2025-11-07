package com.twilight.backend.controller;

import com.twilight.backend.model.ApiResponse;
import com.twilight.backend.model.KlineIndicatorMetric;
import com.twilight.backend.model.KlineMetric;
import com.twilight.backend.repository.KlineRepository;
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
 * K线指标API
 */
@Slf4j
@RestController
@RequestMapping("/v1/klines")
@RequiredArgsConstructor
@Tag(name = "Kline Analytics API", description = "标准K线及指标查询接口")
public class KlineAnalyticsController {

    private final KlineRepository klineRepository;

    @GetMapping("/markets")
    @Operation(summary = "获取K线最新快照", description = "聚合最新一根已收盘K线，用于大盘展示与排序")
    public ApiResponse<List<KlineMetric>> getMarketSnapshots(
            @RequestParam(required = false) List<String> symbols,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, defaultValue = "1m") String interval,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "50") Integer pageSize,
            @RequestParam(defaultValue = "volume") String sortBy,
            @RequestParam(defaultValue = "desc") String order) {

        List<String> normalizedSymbols = normalizeQueryList(symbols);

        try {
            List<KlineMetric> candles = klineRepository.findLatestKlines(
                    normalizedSymbols,
                    exchange,
                    interval,
                    page != null ? page : 1,
                    pageSize != null ? pageSize : 50,
                    sortBy,
                    order);
            return ApiResponse.success(candles);
        } catch (Exception ex) {
            log.error("获取K线快照失败 symbols={}, exchange={}, interval={}",
                    normalizedSymbols, exchange, interval, ex);
            return ApiResponse.serverError("获取K线快照失败: " + ex.getMessage());
        }
    }

    @GetMapping("/{symbol}/candles")
    @Operation(summary = "获取K线时间序列", description = "返回指定交易对的历史K线，用于绘图展示")
    public ApiResponse<List<KlineMetric>> getKlineSeries(
            @PathVariable String symbol,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, defaultValue = "1m") String interval,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "1000") Integer limit) {

        try {
            List<KlineMetric> series = klineRepository.findKlineSeries(
                    symbol,
                    exchange,
                    interval,
                    startTime,
                    endTime,
                    limit != null ? limit : 1000);
            return ApiResponse.success(series);
        } catch (Exception ex) {
            log.error("获取K线序列失败 symbol={}, exchange={}, interval={}", symbol, exchange, interval, ex);
            return ApiResponse.serverError("获取K线序列失败: " + ex.getMessage());
        }
    }

    @GetMapping("/{symbol}/indicators")
    @Operation(summary = "获取K线指标序列", description = "返回RSI/MACD等指标时间序列，可多指标同时过滤")
    public ApiResponse<List<KlineIndicatorMetric>> getIndicatorSeries(
            @PathVariable String symbol,
            @RequestParam(required = false) String exchange,
            @RequestParam(required = false, defaultValue = "1m") String interval,
            @RequestParam(required = false) List<String> indicators,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime,
            @RequestParam(required = false, defaultValue = "1000") Integer limit) {

        List<String> normalizedIndicators = normalizeQueryList(indicators);

        try {
            List<KlineIndicatorMetric> series = klineRepository.findIndicatorSeries(
                    symbol,
                    exchange,
                    interval,
                    normalizedIndicators,
                    startTime,
                    endTime,
                    limit != null ? limit : 1000);
            return ApiResponse.success(series);
        } catch (Exception ex) {
            log.error("获取K线指标失败 symbol={}, exchange={}, interval={}, indicators={}",
                    symbol, exchange, interval, normalizedIndicators, ex);
            return ApiResponse.serverError("获取K线指标失败: " + ex.getMessage());
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
