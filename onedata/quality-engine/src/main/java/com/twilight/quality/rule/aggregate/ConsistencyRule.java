package com.twilight.quality.rule.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.aggregator.WindowState;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.AggregateRule;

import java.util.*;

/**
 * 一致性规则
 * 检测序列号/时间戳的连续性
 */
public abstract class ConsistencyRule extends AggregateRule {
    
    protected static final String ATTR_SEQUENCES = "sequences";
    protected static final String ATTR_GAP_COUNT = "gap_count";
    protected static final String ATTR_LAST_SEQ = "last_seq";
    
    /**
     * 允许的最大Gap数量
     */
    protected int maxGaps = 0;
    
    /**
     * 序列号字段名
     */
    protected String sequenceField = "sequence";
    
    protected ConsistencyRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.CONSISTENCY, domains);
        this.defaultAlertLevel = AlertLevel.WARNING;
    }
    
    @Override
    public void accumulate(JsonNode message, RuleContext context, WindowState state) {
        state.incrementCount();
        
        // 提取序列号
        Long sequence = extractSequence(message);
        if (sequence == null) {
            return;
        }
        
        // 获取序列号列表
        @SuppressWarnings("unchecked")
        List<Long> sequences = state.getAttribute(ATTR_SEQUENCES, new ArrayList<>());
        sequences.add(sequence);
        state.setAttribute(ATTR_SEQUENCES, sequences);
        
        // 检测Gap
        Long lastSeq = state.getAttribute(ATTR_LAST_SEQ);
        if (lastSeq != null && sequence > lastSeq + 1) {
            int gapCount = state.getAttribute(ATTR_GAP_COUNT, 0);
            state.setAttribute(ATTR_GAP_COUNT, gapCount + 1);
        }
        state.setAttribute(ATTR_LAST_SEQ, sequence);
    }
    
    /**
     * 从消息中提取序列号
     */
    protected Long extractSequence(JsonNode message) {
        if (message.has(sequenceField)) {
            JsonNode seqNode = message.get(sequenceField);
            if (seqNode.isNumber()) {
                return seqNode.asLong();
            }
        }
        
        // 尝试其他常见字段
        String[] fallbackFields = {"update_id", "seq", "id", "event_id", "block_number"};
        for (String field : fallbackFields) {
            if (message.has(field)) {
                JsonNode node = message.get(field);
                if (node.isNumber()) {
                    return node.asLong();
                }
            }
        }
        
        return null;
    }
    
    @Override
    public Optional<RuleResult> evaluateWindow(WindowState state) {
        long messageCount = state.getMessageCount();
        
        if (messageCount == 0) {
            return Optional.empty();
        }
        
        int gapCount = state.getAttribute(ATTR_GAP_COUNT, 0);
        
        @SuppressWarnings("unchecked")
        List<Long> sequences = state.getAttribute(ATTR_SEQUENCES, new ArrayList<>());
        
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("message_count", messageCount);
        ctx.put("gap_count", gapCount);
        ctx.put("max_gaps_allowed", maxGaps);
        
        // 计算序列号范围
        if (!sequences.isEmpty()) {
            Collections.sort(sequences);
            ctx.put("min_sequence", sequences.get(0));
            ctx.put("max_sequence", sequences.get(sequences.size() - 1));
            ctx.put("expected_count", sequences.get(sequences.size() - 1) - sequences.get(0) + 1);
        }
        
        // 判断是否有Gap
        if (gapCount > maxGaps) {
            String message = String.format("检测到序列号Gap: gap数量=%d (允许=%d), 消息数=%d",
                    gapCount, maxGaps, messageCount);
            
            RuleResult result = createWindowResult(state, false, message, 
                    (double) gapCount, (double) maxGaps);
            result.setContext(ctx);
            return Optional.of(result);
        }
        
        return Optional.of(createWindowResult(state, true,
                String.format("序列一致性正常: 消息数=%d, Gap数=%d", messageCount, gapCount),
                (double) gapCount, (double) maxGaps));
    }
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("max_gaps")) {
            this.maxGaps = Integer.parseInt(config.get("max_gaps").toString());
        }
        if (config.containsKey("sequence_field")) {
            this.sequenceField = config.get("sequence_field").toString();
        }
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX序列一致性规则
     */
    public static class DexConsistencyRule extends ConsistencyRule {
        
        public DexConsistencyRule() {
            super("dex.consistency.sequence", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            this.sequenceField = "block_number";
            this.maxGaps = 0; // 区块不允许Gap
            this.windowSizeMs = 60000;
        }
        
        @Override
        protected Long extractSequence(JsonNode message) {
            // DEX使用block_number作为序列
            if (message.has("transaction")) {
                JsonNode tx = message.get("transaction");
                if (tx.has("block_number")) {
                    return tx.get("block_number").asLong();
                }
            }
            return super.extractSequence(message);
        }
    }
    
    /**
     * 订单簿序列一致性规则
     */
    public static class OrderbookConsistencyRule extends ConsistencyRule {
        
        public OrderbookConsistencyRule() {
            super("perp.orderbook.consistency", DataDomain.CEX_PERP_ORDERBOOK);
            
            this.sequenceField = "update_id";
            this.maxGaps = 5; // 允许少量Gap（网络抖动）
            this.windowSizeMs = 60000;
        }
    }
}

