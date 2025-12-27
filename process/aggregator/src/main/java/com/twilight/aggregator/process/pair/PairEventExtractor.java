// package com.twilight.aggregator.process.pair;

// import org.apache.flink.api.common.functions.RichFlatMapFunction;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.util.Collector;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.twilight.aggregator.model.ProcessEvent;
// import com.twilight.aggregator.model.Pair;
// import com.twilight.aggregator.utils.EthereumUtils;

// import java.util.Map;

// /**
//  * Pair事件提取器
//  * 从ProcessEvent中提取Pair相关的事件数据
//  * 处理Swap、Sync、Mint、Burn等DEX交易对事件
//  */
// public class PairEventExtractor extends RichFlatMapFunction<ProcessEvent, Pair> {
//     private static final Logger log = LoggerFactory.getLogger(PairEventExtractor.class);
    
//     // 统计计数器
//     private transient long processedEvents = 0;
//     private transient long extractedPairs = 0;
    
//     @Override
//     public void open(Configuration parameters) throws Exception {
//         super.open(parameters);
//         log.info("🚀 PairEventExtractor initialized");
//     }
    
//     @Override
//     public void flatMap(ProcessEvent event, Collector<Pair> out) throws Exception {
//         processedEvents++;
        
//         try {
//             // 验证事件基本信息（不再用 decodedArgs 作为先决条件）
//             if (event == null) {
//                 return;
//             }
            
//             String eventName = event.getEventName();
//             if (eventName == null) {
//                 return;
//             }
            
//             // 只处理Pair相关的事件
//             if (!isPairEvent(eventName)) {
//                 return;
//             }
            
//             // 创建Pair对象
//             Pair pair = createPairFromEvent(event);
//             if (pair != null) {
//                 out.collect(pair);
//                 extractedPairs++;
                
//                 log.trace("✅ Extracted pair event: {} from contract {}", 
//                          eventName, event.getContractAddress());
//             }
            
//             // 定期打印统计信息
//             if (processedEvents % 1000 == 0) {
//                 log.info("📊 PairEventExtractor: Processed {} events, extracted {} pairs", 
//                         processedEvents, extractedPairs);
//             }
            
//         } catch (Exception e) {
//             log.error("💥 Error processing pair event: {}", e.getMessage(), e);
//         }
//     }
    
//     /**
//      * 判断是否为Pair相关事件
//      */
//     private boolean isPairEvent(String eventName) {
//         return "Swap".equals(eventName) || 
//                "Sync".equals(eventName) || 
//                "Mint".equals(eventName) || 
//                "Burn".equals(eventName);
//     }
    
//     /**
//      * 从ProcessEvent创建Pair对象
//      */
//     private Pair createPairFromEvent(ProcessEvent event) {
//         try {
//             Map<String, String> decodedArgs = event.getDecodedArgs();
            
//             Pair pair = new Pair();
            
//             // 设置基础字段
//             pair.setPairAddress(event.getContractAddress());
//             pair.setEventName(event.getEventName());
//             pair.setFromAddress(event.getFromAddress());
//             pair.setTimestamp(event.getTimestamp());
            
//             // 根据事件类型获取相应的强类型数据
//             ProcessEvent.EventType eventType = event.getEventType();
//             switch (eventType) {
//                 case DEX_SWAP:
//                     setSwapPairFields(pair, event.getDexSwapData());
//                     break;
//                 case LP_MINT:
//                     setMintPairFields(pair, event.getLpMintData());
//                     break;
//                 case LP_BURN:
//                     setBurnPairFields(pair, event.getLpBurnData());
//                     break;
//                 default:
//                     // 对于Sync等其他事件，尝试从基础字段获取
//                     setPairFieldsFromBasicEvent(pair, event);
//                     break;
//             }
            
//             // 根据事件名称设置特定字段（保持向后兼容）
//             switch (event.getEventName()) {
//                 case "Sync":
//                     if (event.getDecodedArgs() != null) {
//                         setSyncFields(pair, event.getDecodedArgs());
//                     }
//                     break;
//                 default:
//                     // Swap/Mint/Burn的字段已在强类型数据中处理
//                     break;
//             }
            
//             // 验证价格数据
//             if (pair.getToken0PriceUsd() == 0 || pair.getToken1PriceUsd() == 0) {
//                 log.debug("⚠️ Pair event with zero price - event: {}, pair: {}", 
//                          event.getEventName(), pair.getPairAddress());
//             }
            
//             return pair;
            
//         } catch (Exception e) {
//             log.error("💥 Error creating pair from event {}: {}", event.getEventName(), e.getMessage());
//             return null;
//         }
//     }
    
//     /**
//      * 设置Swap事件的字段
//      */
//     private void setSwapFields(Pair pair, Map<String, String> decodedArgs) {
//         // 转换Wei到Eth并设置交易量
//         pair.setAmount0In(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0In")));
//         pair.setAmount0Out(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0Out")));
//         pair.setAmount1In(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1In")));
//         pair.setAmount1Out(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1Out")));
//     }
    
//     /**
//      * 设置Sync事件的字段
//      */
//     private void setSyncFields(Pair pair, Map<String, String> decodedArgs) {
//         // 转换Wei到Eth并设置储备量
//         pair.setReserve0(EthereumUtils.convertWeiToEth(decodedArgs.get("reserve0")));
//         pair.setReserve1(EthereumUtils.convertWeiToEth(decodedArgs.get("reserve1")));
//     }
    
//     /**
//      * 设置Mint/Burn事件的字段（已废弃，使用强类型数据）
//      */
//     private void setMintBurnFields(Pair pair, Map<String, String> decodedArgs) {
//         // 这个方法已经被强类型数据处理方法替代，保留以防万一
//         pair.setAmount0(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0")));
//         pair.setAmount1(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1")));
//     }
    
//     /**
//      * 从Swap强类型数据设置Pair字段
//      */
//     private void setSwapPairFields(Pair pair, ProcessEvent.DexSwapData swapData) {
//         if (swapData == null) {
//             return;
//         }
        
//         pair.setPairId(swapData.getPairId());
//         pair.setToken0Address(swapData.getToken0Address());
//         pair.setToken1Address(swapData.getToken1Address());
//         pair.setToken0PriceUsd(swapData.getToken0PriceUsd());
//         pair.setToken1PriceUsd(swapData.getToken1PriceUsd());
        
//         // 设置Swap特定字段
//         if (swapData.getAmount0In() != null) {
//             pair.setAmount0In(swapData.getAmount0In().doubleValue());
//         }
//         if (swapData.getAmount0Out() != null) {
//             pair.setAmount0Out(swapData.getAmount0Out().doubleValue());
//         }
//         if (swapData.getAmount1In() != null) {
//             pair.setAmount1In(swapData.getAmount1In().doubleValue());
//         }
//         if (swapData.getAmount1Out() != null) {
//             pair.setAmount1Out(swapData.getAmount1Out().doubleValue());
//         }
//     }
    
//     /**
//      * 从Mint强类型数据设置Pair字段
//      */
//     private void setMintPairFields(Pair pair, ProcessEvent.LPMintData mintData) {
//         if (mintData == null) {
//             return;
//         }
        
//         pair.setPairId(mintData.getPairId());
//         pair.setToken0Address(mintData.getToken0Address());
//         pair.setToken1Address(mintData.getToken1Address());
//         pair.setToken0PriceUsd(mintData.getToken0PriceUsd());
//         pair.setToken1PriceUsd(mintData.getToken1PriceUsd());
        
//         // 设置Mint特定字段
//         if (mintData.getAmount0() != null) {
//             pair.setAmount0(mintData.getAmount0().doubleValue());
//         }
//         if (mintData.getAmount1() != null) {
//             pair.setAmount1(mintData.getAmount1().doubleValue());
//         }
//     }
    
//     /**
//      * 从Burn强类型数据设置Pair字段
//      */
//     private void setBurnPairFields(Pair pair, ProcessEvent.LPBurnData burnData) {
//         if (burnData == null) {
//             return;
//         }
        
//         pair.setPairId(burnData.getPairId());
//         pair.setToken0Address(burnData.getToken0Address());
//         pair.setToken1Address(burnData.getToken1Address());
//         pair.setToken0PriceUsd(burnData.getToken0PriceUsd());
//         pair.setToken1PriceUsd(burnData.getToken1PriceUsd());
        
//         // 设置Burn特定字段
//         if (burnData.getAmount0() != null) {
//             pair.setAmount0(burnData.getAmount0().doubleValue());
//         }
//         if (burnData.getAmount1() != null) {
//             pair.setAmount1(burnData.getAmount1().doubleValue());
//         }
//     }
    
//     /**
//      * 从基础事件设置Pair字段（用于Sync等事件）
//      */
//     private void setPairFieldsFromBasicEvent(Pair pair, ProcessEvent event) {
//         // 对于Sync等事件，如果有pair metadata可以使用
//         if (event.getPairMetadata() != null) {
//             // 这里可以从pairMetadata中提取信息
//             // 暂时留空，具体实现取决于metadata的结构
//             log.trace("📝 Processing basic event {} for pair {}", event.getEventName(), event.getContractAddress());
//         }
//     }
// }
