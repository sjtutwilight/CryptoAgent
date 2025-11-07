package com.twilight.backend.repository;

import com.twilight.backend.model.KlineIndicatorMetric;
import com.twilight.backend.model.KlineMetric;

import java.time.LocalDateTime;
import java.util.List;

/**
 * K线指标查询接口
 */
public interface KlineRepository {

    /**
     * 查询最新的K线快照（用于大盘列表）
     *
     * @param symbols   可选交易对过滤
     * @param exchange  交易所
     * @param interval  K线周期，如 "1m" / "5m"
     * @param page      页码（从1开始）
     * @param pageSize  页大小
     * @param sortBy    排序字段
     * @param order     排序方向
     * @return          最新K线列表
     */
    List<KlineMetric> findLatestKlines(List<String> symbols,
                                       String exchange,
                                       String interval,
                                       int page,
                                       int pageSize,
                                       String sortBy,
                                       String order);

    /**
     * 查询指定交易对的K线时间序列
     */
    List<KlineMetric> findKlineSeries(String symbol,
                                      String exchange,
                                      String interval,
                                      LocalDateTime startTime,
                                      LocalDateTime endTime,
                                      int limit);

    /**
     * 查询指定交易对的指标时间序列
     */
    List<KlineIndicatorMetric> findIndicatorSeries(String symbol,
                                                   String exchange,
                                                   String interval,
                                                   List<String> indicators,
                                                   LocalDateTime startTime,
                                                   LocalDateTime endTime,
                                                   int limit);
}
