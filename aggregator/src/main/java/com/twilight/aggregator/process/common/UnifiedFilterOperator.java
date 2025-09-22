package com.twilight.aggregator.process.common;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.dexTransaction.Event;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
import com.twilight.aggregator.utils.EthereumUtils;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 统一的事件过滤算子
 * 直接从KafkaMessage中提取并过滤事件，输出ProcessEvent
 * 整合了事件提取和过滤逻辑，减少算子链路
 */
public class UnifiedFilterOperator extends RichFlatMapFunction<KafkaMessage, ProcessEvent> {
    private static final Logger log = LoggerFactory.getLogger(UnifiedFilterOperator.class);
    
    /**
     * 过滤配置枚举
     */
    public enum FilterType {
        /** 仅处理Swap事件 - 用于PnL和Token分析 */
        SWAP_ONLY,
        /** 处理所有DEX事件 - 用于完整聚合分析 */
        ALL_DEX_EVENTS,
        /** 仅处理余额相关事件 - 用于账户余额跟踪 */
        BALANCE_EVENTS,
        /** 自定义事件过滤 */
        CUSTOM
    }
    
    private final FilterType filterType;
    private final Set<String> allowedEvents;
    private final Set<String> blockedEvents;
    
    // 统计计数器
    private transient long processedMessages = 0;
    private transient long extractedEvents = 0;
    private transient long passedEvents = 0;
    private transient long rejectedByEventType = 0;
    
    private UnifiedFilterOperator(Builder builder) {
        this.filterType = builder.filterType;
        this.allowedEvents = builder.allowedEvents;
        this.blockedEvents = builder.blockedEvents;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("🔧 UnifiedFilterOperator initialized with type: {}", filterType);
        log.info("   📋 Allowed events: {}", allowedEvents);
        log.info("   🚫 Blocked events: {}", blockedEvents);
    }
    
    @Override
    public void flatMap(KafkaMessage message, Collector<ProcessEvent> out) throws Exception {
        processedMessages++;
        
        try {
            // 基础验证
            if (message.getEvents() == null || message.getTransaction() == null) {
                log.trace("⚠️ Skipping message with null events or transaction");
                return;
            }
            
            String fromAddress = message.getTransaction().getFromAddress();
            Long timestamp = message.getTransaction().getTimestamp();
            Long blockId = message.getTransaction().getBlockNumber();
            String transactionHash = message.getTransaction().getTransactionHash();
            
            // 处理每个事件
            for (Event event : message.getEvents()) {
                extractedEvents++;
                
                // 事件类型过滤
                if (!isEventAllowed(event.getEventName())) {
                    rejectedByEventType++;
                    log.trace("🚫 Event rejected by type filter: {}", event.getEventName());
                    continue;
                }
                
                // 创建ProcessEvent
                ProcessEvent processEvent = createProcessEvent(event, fromAddress, timestamp, blockId, transactionHash);
                if (processEvent != null) {
                    out.collect(processEvent);
                    passedEvents++;
                    log.trace("✅ Passed event: {} from contract {}", 
                             event.getEventName(), event.getContractAddress());
                }
            }
            
            // 每1000个消息记录一次统计
            if (processedMessages % 1000 == 0) {
                logStatistics();
            }
            
        } catch (Exception e) {
            log.error("💥 Error processing message: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 创建ProcessEvent并设置对应的强类型事件数据
     */
    private ProcessEvent createProcessEvent(Event event, String fromAddress, Long timestamp, Long blockId, String transactionHash) {
        try {
            // 基础验证
            if (event.getEventName() == null || event.getContractAddress() == null || 
                fromAddress == null || timestamp == null) {
                log.trace("⚠️ Skipping event with missing required fields");
                return null;
            }
            
            ProcessEvent processEvent = new ProcessEvent();
            processEvent.setEventName(event.getEventName());
            processEvent.setContractAddress(event.getContractAddress());
            // decodedArgs 仅用于少量场景（如 Sync 的备用解析），不再作为校验依据
            processEvent.setFromAddress(fromAddress);
            processEvent.setTimestamp(timestamp);
            processEvent.setBlockId(blockId);
            processEvent.setTransactionHash(transactionHash);
            processEvent.setLogIndex(event.getLogIndex());
            // 根据事件类型设置对应的强类型数据
            setEventSpecificData(processEvent, event);
            
            return processEvent;
            
        } catch (Exception e) {
            log.error("💥 Error creating ProcessEvent: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 根据事件类型设置对应的强类型数据
     */
    private void setEventSpecificData(ProcessEvent processEvent, Event event) {
        String eventName = event.getEventName();
        Map<String, String> args = event.getDecodedArgs();
        
        try {
            switch (eventName) {
                case "Transfer":
                    setTransferData(processEvent, args);
                    break;
                case "Swap":
                    setSwapData(processEvent, args);
                    break;
                case "Mint":
                    setMintData(processEvent, args);
                    break;
                case "Burn":
                    setBurnData(processEvent, args);
                    break;
                default:
                    log.trace("⚠️ Unknown event type for strong typing: {}", eventName);
                    break;
            }
        } catch (Exception e) {
            log.warn("⚠️ Failed to set event-specific data for {}: {}", eventName, e.getMessage());
        }
    }
    
    /**
     * 设置Transfer事件数据（可能是ERC20或LP Token）
     * 在这个阶段还不能确定是ERC20还是LP，这将在AsyncEventEnrichmentProcessor中确定
     */
    private void setTransferData(ProcessEvent processEvent, Map<String, String> args) {
        ProcessEvent.ERC20TransferData transferData = new ProcessEvent.ERC20TransferData();
        
        transferData.setToAddress(args.get("to"));
        
        // 转换金额
        String valueStr = args.get("value");
        if (valueStr != null) {
            try {
                BigDecimal amount = EthereumUtils.convertWeiToEthBD(valueStr);
                transferData.setAmount(amount);
            } catch (Exception e) {
                log.warn("⚠️ Failed to parse transfer amount {}: {}", valueStr, e.getMessage());
            }
        }
        
        // assetType和bizId将在AsyncEventEnrichmentProcessor中设置
        
        processEvent.setErc20Data(transferData);
    }
    
    /**
     * 设置Swap事件数据
     */
    private void setSwapData(ProcessEvent processEvent, Map<String, String> args) {
        ProcessEvent.DexSwapData swapData = new ProcessEvent.DexSwapData();
        
        swapData.setTo(args.get("to"));
        
        // 转换金额字段
        try {
            if (args.get("amount0In") != null) {
                swapData.setAmount0In(EthereumUtils.convertWeiToEthBD(args.get("amount0In")));
            }
            if (args.get("amount0Out") != null) {
                swapData.setAmount0Out(EthereumUtils.convertWeiToEthBD(args.get("amount0Out")));
            }
            if (args.get("amount1In") != null) {
                swapData.setAmount1In(EthereumUtils.convertWeiToEthBD(args.get("amount1In")));
            }
            if (args.get("amount1Out") != null) {
                swapData.setAmount1Out(EthereumUtils.convertWeiToEthBD(args.get("amount1Out")));
            }
        } catch (Exception e) {
            log.warn("⚠️ Failed to parse swap amounts: {}", e.getMessage());
        }
        
        processEvent.setDexSwapData(swapData);
    }
    
    /**
     * 设置Mint事件数据
     */
    private void setMintData(ProcessEvent processEvent, Map<String, String> args) {
        ProcessEvent.LPMintData mintData = new ProcessEvent.LPMintData();
        
        mintData.setSender(args.get("sender"));
        mintData.setTo(args.get("to"));
        
        // 转换金额字段
        try {
            if (args.get("amount0") != null) {
                mintData.setAmount0(EthereumUtils.convertWeiToEthBD(args.get("amount0")));
            }
            if (args.get("amount1") != null) {
                mintData.setAmount1(EthereumUtils.convertWeiToEthBD(args.get("amount1")));
            }
        } catch (Exception e) {
            log.warn("⚠️ Failed to parse mint amounts: {}", e.getMessage());
        }
        
        processEvent.setLpMintData(mintData);
    }
    
    /**
     * 设置Burn事件数据
     */
    private void setBurnData(ProcessEvent processEvent, Map<String, String> args) {
        ProcessEvent.LPBurnData burnData = new ProcessEvent.LPBurnData();
        
        burnData.setSender(args.get("sender"));
        burnData.setTo(args.get("to"));
        
        // 转换金额字段
        try {
            if (args.get("amount0") != null) {
                burnData.setAmount0(EthereumUtils.convertWeiToEthBD(args.get("amount0")));
            }
            if (args.get("amount1") != null) {
                burnData.setAmount1(EthereumUtils.convertWeiToEthBD(args.get("amount1")));
            }
        } catch (Exception e) {
            log.warn("⚠️ Failed to parse burn amounts: {}", e.getMessage());
        }
        
        processEvent.setLpBurnData(burnData);
    }
    
    /**
     * 检查事件类型是否被允许
     */
    private boolean isEventAllowed(String eventName) {

        // 如果有明确的允许列表，检查是否在其中
        if (!allowedEvents.isEmpty()) {
            return allowedEvents.contains(eventName);
        }
        
        // 默认过滤策略
        switch (filterType) {
            case SWAP_ONLY:
                return "Swap".equals(eventName);
            case BALANCE_EVENTS:
                return "Transfer".equals(eventName) ;
            case ALL_DEX_EVENTS:
                return "Swap".equals(eventName) || 
                       "Sync".equals(eventName) || 
                       "Mint".equals(eventName) || 
                       "Burn".equals(eventName);
            case CUSTOM:
                return true; // 依赖allowedEvents和blockedEvents配置
            default:
                return false;
        }
    }
    
    /**
     * 记录统计信息
     */
    private void logStatistics() {
        double extractionRate = processedMessages > 0 ? (double) extractedEvents / processedMessages : 0;
        double passRate = extractedEvents > 0 ? (double) passedEvents / extractedEvents * 100 : 0;
        
        log.info("📊 UnifiedFilterOperator Stats:");
        log.info("   📨 Messages: {}, Events extracted: {} (avg {:.1f}/msg)", 
                processedMessages, extractedEvents, extractionRate);
        log.info("   ✅ Events passed: {} ({:.1f}%)", passedEvents, passRate);
        log.info("   🚫 Events rejected by type: {}", rejectedByEventType);
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        double passRate = extractedEvents > 0 ? (double) passedEvents / extractedEvents * 100 : 0;
        log.info("🛑 UnifiedFilterOperator closed. Final stats:");
        log.info("   📨 Messages processed: {}", processedMessages);
        log.info("   📊 Events extracted: {}", extractedEvents);
        log.info("   ✅ Events passed: {} ({:.1f}%)", passedEvents, passRate);
        log.info("   🚫 Events rejected by type: {}", rejectedByEventType);
    }
    
    
    /**
     * 建造者模式构建过滤器
     */
    public static class Builder {
        private final FilterType filterType;
        private Set<String> allowedEvents = new HashSet<>();
        private Set<String> blockedEvents = new HashSet<>();
        
        public Builder(FilterType filterType) {
            this.filterType = filterType;
            // 默认阻止Approval事件
            this.blockedEvents.add("Approval");
        }
        
        public Builder allowEvent(String eventName) {
            this.allowedEvents.add(eventName);
            return this;
        }
        
        public Builder allowEvents(String... eventNames) {
            for (String eventName : eventNames) {
                this.allowedEvents.add(eventName);
            }
            return this;
        }
        
        public Builder blockEvent(String eventName) {
            this.blockedEvents.add(eventName);
            return this;
        }
        
        public Builder blockEvents(String... eventNames) {
            for (String eventName : eventNames) {
                this.blockedEvents.add(eventName);
            }
            return this;
        }
        
        public UnifiedFilterOperator build() {
            return new UnifiedFilterOperator(this);
        }
    }
    
    /**
     * 工厂类，提供预配置的过滤器
     */
    public static class Factory {
        /**
         * 余额跟踪专用过滤器 - 只处理Transfer事件
         * Transfer事件包含所有余额变化信息：
         * - Transfer: 正常转账
         * - Mint: from=0x0的Transfer事件  
         * - Burn: to=0x0的Transfer事件
         */
        public static UnifiedFilterOperator forBalanceTracking() {
            return new Builder(FilterType.BALANCE_EVENTS)
                .allowEvents("Transfer")  // 只允许Transfer事件
                .blockEvents("Approval", "Sync", "Swap")  // 明确阻止非余额相关事件
                .build();
        }
        
        /**
         * 创建用于PnL分析的过滤器 - 仅Swap事件
         */
        public static UnifiedFilterOperator forPnLAnalysis() {
            return new Builder(FilterType.SWAP_ONLY).build();
        }
        
        /**
         * 创建用于Token分析的过滤器 - 仅Swap事件
         */
        public static UnifiedFilterOperator forTokenAnalysis() {
            return new Builder(FilterType.SWAP_ONLY).build();
        }
        
        /**
         * 创建用于Pair分析的过滤器 - 所有DEX事件
         */
        public static UnifiedFilterOperator forPairAnalysis() {
            return new Builder(FilterType.ALL_DEX_EVENTS).build();
        }
        
        /**
         * 交易跟踪专用过滤器 - 包含DEX相关事件
         */
        public static UnifiedFilterOperator forTradeTracking() {
            return new Builder(FilterType.ALL_DEX_EVENTS)
                .allowEvents("Swap", "Transfer", "Mint", "Burn")
                .blockEvents("Approval", "Sync")
                .build();
        }
        
        /**
         * 通用过滤器 - 允许所有DEX相关事件
         */
        public static UnifiedFilterOperator forGeneralProcessing() {
            return new Builder(FilterType.ALL_DEX_EVENTS)
                .allowEvents("Transfer", "Swap", "Mint", "Burn", "Sync")
                .blockEvents("Approval")
                .build();
        }
        
        /**
         * 交易事实表专用过滤器 - 仅Swap事件，用于TradeFactJob
         */
        public static UnifiedFilterOperator forTradeFactProcessing() {
            return new Builder(FilterType.SWAP_ONLY).build();
        }
    }
}
