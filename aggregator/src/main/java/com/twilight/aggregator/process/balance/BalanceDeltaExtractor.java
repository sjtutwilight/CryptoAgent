package com.twilight.aggregator.process.balance;

import com.twilight.aggregator.model.AccountMetadata;
import com.twilight.aggregator.model.BalanceDelta;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.utils.EthereumUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * Balance Delta提取器 - 标准化版本
 * 从已增强的ProcessEvent中提取余额变化，元数据已由前序算子处理
 * 
 * 处理流程：
 * 1. 接收已增强的ProcessEvent（包含元数据和价格信息）
 * 2. 只处理Transfer事件，根据from/to地址判断mint/burn/transfer类型
 * 3. 从ProcessEvent的强类型数据中提取资产信息
 * 4. 从accountMetadata获取accountId，确保keyBy字段不为null
 * 5. 生成完整的BalanceDelta对象
 */
public class BalanceDeltaExtractor extends RichFlatMapFunction<ProcessEvent, BalanceDelta> {
    private static final Logger log = LoggerFactory.getLogger(BalanceDeltaExtractor.class);
    
    // 统计计数器
    private transient long processedEvents = 0;
    private transient long extractedDeltas = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("🚀 BalanceDeltaExtractor initialized for ProcessEvent stream");
    }
    
    @Override
    public void flatMap(ProcessEvent event, Collector<BalanceDelta> out) throws Exception {
        processedEvents++;
        
        try {
            log.info("🔹 BalanceDeltaExtractor event={}", event);
            // 处理Transfer事件
            processTransferEvent(event, out);
            // 定期打印统计信息
            if (processedEvents % 1000 == 0) {
                log.info("📊 BalanceDeltaExtractor: Processed {} events, extracted {} deltas", 
                        processedEvents, extractedDeltas);
            }
            
        } catch (Exception e) {
            log.error("💥 Error processing balance delta event: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 处理Transfer事件，提取余额变化
     */
    private void processTransferEvent(ProcessEvent event, Collector<BalanceDelta> out) {
        try {
            ProcessEvent.ERC20TransferData transfer = event.getErc20Data();
            String fromAddress = event.getFromAddress() != null ? event.getFromAddress().toLowerCase() : null;
            String toAddress = transfer.getToAddress() != null ? transfer.getToAddress().toLowerCase() : null;
            BigDecimal value = transfer.getAmount();


            if (fromAddress == null || toAddress == null || value == null) {
                log.trace("⚠️ Incomplete Transfer event data (typed)");
                return;
            }

            // 检查是否为空地址（判断是mint/burn）
            boolean isMint = "0x0000000000000000000000000000000000000000".equals(fromAddress);
            boolean isBurn = "0x0000000000000000000000000000000000000000".equals(toAddress);
 
            
            // 生成余额变化事件的时间戳
            LocalDateTime eventTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(event.getTimestamp()),
                ZoneId.systemDefault()
            );
            
            // 为from地址生成负的delta（如果不是mint）
            if (!isMint) {
                BalanceDelta fromDelta = createBalanceDelta(
                    event, value.negate(), eventTime
                );
                
                if (fromDelta != null) {
                    out.collect(fromDelta);
                    extractedDeltas++;
                    
                }
            }
            
            // 为to地址生成正的delta（如果不是burn）
            if (!isBurn) {
                BalanceDelta toDelta = createBalanceDelta(
                    event, value, eventTime
                );
                
                if (toDelta != null) {
                    out.collect(toDelta);
                    extractedDeltas++;
                    
                }
            }
            
        } catch (Exception e) {
            log.error("💥 Error processing transfer event for contract {}: {}", 
                     event.getContractAddress(), e.getMessage());
        }
    }
    
    /**
     * 创建BalanceDelta对象
     */
    private BalanceDelta createBalanceDelta(ProcessEvent event, 
                                      BigDecimal delta, LocalDateTime eventTime
                         ) {
        try {
            BalanceDelta balanceDelta = new BalanceDelta();
            
            // 设置基础信息
            balanceDelta.setAccountId(event.getAccountMetadata().getId()); 
            balanceDelta.setAccountAddress(event.getAccountMetadata().getAddress());
            
            // 设置资产信息
            balanceDelta.setAssetType(event.getContractType());
            balanceDelta.setBizId(event.getBizId());
            balanceDelta.setBizName("UNKNOWN");
            balanceDelta.setContractAddress(event.getContractAddress());
            
            // 设置变化信息
            balanceDelta.setDelta(delta);
            balanceDelta.setEventTime(eventTime);
            
            // 设置区块链信息
            balanceDelta.setBlockId(event.getBlockId() != null ? event.getBlockId() : 0L);
            balanceDelta.setTransactionHash(event.getTransactionHash() != null ? event.getTransactionHash() : "");
            
            // 设置辅助字段
            balanceDelta.setFromAddress(event.getFromAddress());
            balanceDelta.setToAddress(event.getErc20Data().getToAddress());
            
            // 验证关键字段不为null（用于keyBy）
            if (balanceDelta.getAccountId() == null || balanceDelta.getAssetType() == null || balanceDelta.getBizId() == null) {
                log.warn("⚠️ Critical field is null - accountId: {}, assetType: {}, bizId: {}, address: {}", 
                        balanceDelta.getAccountId(), balanceDelta.getAssetType(), balanceDelta.getBizId(), balanceDelta.getAccountAddress());
                return null; // 返回null而不是invalid对象
            }
            
            return balanceDelta;
            
        } catch (Exception e) {
            log.error("💥 Error creating BalanceDelta: {}", e.getMessage());
            return null;
        }
    }
    
    
    @Override
    public void close() throws Exception {
        super.close();
        log.info("🛑 BalanceDeltaExtractor closed. Final stats - Processed: {}, Extracted: {}", 
                processedEvents, extractedDeltas);
    }
}
