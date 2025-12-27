package com.twilight.backend.repository;

import com.twilight.backend.model.PerpContextMetric;
import com.twilight.backend.model.PerpExecutionMetric;
import com.twilight.backend.model.PerpPanelMetric;
import com.twilight.backend.model.PerpSignal;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 永续合约数据访问接口
 */
public interface PerpRepository {

    /**
     * 获取最新的汇合面板快照列表（用于大盘展示）
     *
     * @param symbols     可选的交易对列表过滤
     * @param exchange    交易所过滤
     * @param algoVersion 算法版本过滤
     * @param page        页码（从1开始）
     * @param pageSize    每页数量
     * @param sortBy      排序字段
     * @param order       排序方向（asc/desc）
     * @return 面板指标列表
     */
    List<PerpPanelMetric> findLatestPanelSnapshots(List<String> symbols,
                                                   String exchange,
                                                   String algoVersion,
                                                   int page,
                                                   int pageSize,
                                                   String sortBy,
                                                   String order);

    /**
     * 查询秒级执行面指标时间序列
     *
     * @param symbol    交易对
     * @param exchange  交易所
     * @param algo      算法版本
     * @param startTime 起始时间
     * @param endTime   结束时间
     * @param limit     限制返回数量
     * @return 执行面指标列表
     */
    List<PerpExecutionMetric> findExecutionMetrics(String symbol,
                                                   String exchange,
                                                   String algo,
                                                   LocalDateTime startTime,
                                                   LocalDateTime endTime,
                                                   int limit);

    /**
     * 查询语境面分钟级指标时间序列
     */
    List<PerpContextMetric> findContextMetrics(String symbol,
                                               String exchange,
                                               String algo,
                                               LocalDateTime startTime,
                                               LocalDateTime endTime,
                                               int limit);

    /**
     * 查询汇合面板时间序列
     */
    List<PerpPanelMetric> findPanelMetrics(String symbol,
                                           String exchange,
                                           String algo,
                                           LocalDateTime startTime,
                                           LocalDateTime endTime,
                                           int limit);

    /**
     * 查询异常信号列表
     *
     * @param symbols     可选交易对过滤
     * @param exchanges   可选交易所过滤
     * @param types       可选信号类型过滤
     * @param levels      可选信号级别过滤
     * @param algoVersion 算法版本过滤
     * @param startTime   时间范围起始
     * @param endTime     时间范围结束
     * @param limit       返回数量限制
     * @return 异常信号列表
     */
    List<PerpSignal> findSignals(List<String> symbols,
                                 List<String> exchanges,
                                 List<String> types,
                                 List<String> levels,
                                 String algoVersion,
                                 LocalDateTime startTime,
                                 LocalDateTime endTime,
                                 int limit);
}
