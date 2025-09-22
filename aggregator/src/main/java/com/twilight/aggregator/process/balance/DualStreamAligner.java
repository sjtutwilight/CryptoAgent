package com.twilight.aggregator.process.balance;

import com.twilight.aggregator.model.AccountBalance;
import com.twilight.aggregator.model.BalanceDelta;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;

/**
 * 双流对齐处理器
 * 实现快照流和增量流的基于block_id的选择性写入逻辑
 * 
 * 规则：
 * 1. 快照流：block_id >= 当前记录的block_id 才写入到ch_account_balance_snapshot
 * 2. 增量流：block_id > 当前记录的block_id 才写入到ch_account_balance_snapshot
 * 
 * 注意：两个流都写入同一张表，通过block_id规则控制写入时机
 */
public class DualStreamAligner extends KeyedCoProcessFunction<Tuple3<Long, String, Long>, AccountBalance, BalanceDelta, AccountBalance> {
    
    private static final Logger log = LoggerFactory.getLogger(DualStreamAligner.class);
    
    // 缓存增量（当该 key 还未收到快照时）
    private transient ListState<BalanceDelta> pendingDeltaQueue;
    
    // 状态：存储每个key最新的快照block_id
    private transient ValueState<Long> snapshotBlockIdState;

    // 当前累计余额（从快照基线开始滚动增量）
    private transient ValueState<BigDecimal> currentAmountState;
    // 最近价格（用于计算 value_usd）
    private transient ValueState<BigDecimal> lastPriceUsdState;
    
    // 统计信息
    private long processedSnapshots = 0;
    private long processedDeltas = 0;
    private long droppedSnapshots = 0;
    private long droppedDeltas = 0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化状态
        ValueStateDescriptor<Long> snapshotBlockIdDescriptor = new ValueStateDescriptor<>(
            "snapshot-block-id",
            TypeInformation.of(Long.class)
        );
        snapshotBlockIdState = getRuntimeContext().getState(snapshotBlockIdDescriptor);

        ListStateDescriptor<BalanceDelta> pendingDesc = new ListStateDescriptor<>(
            "pending-deltas",
            TypeInformation.of(BalanceDelta.class)
        );
        pendingDeltaQueue = getRuntimeContext().getListState(pendingDesc);

        ValueStateDescriptor<BigDecimal> currentAmountDesc = new ValueStateDescriptor<>(
            "current-amount",
            TypeInformation.of(BigDecimal.class)
        );
        currentAmountState = getRuntimeContext().getState(currentAmountDesc);

        ValueStateDescriptor<BigDecimal> lastPriceDesc = new ValueStateDescriptor<>(
            "last-price-usd",
            TypeInformation.of(BigDecimal.class)
        );
        lastPriceUsdState = getRuntimeContext().getState(lastPriceDesc);
        
        log.info("🔄 DualStreamAligner initialized");
    }
    
    /**
     * 处理快照流数据
     * 规则：block_id >= 当前存储的block_id 才写入
     * 输出：直接输出AccountBalance到ClickHouse
     */
    @Override
    public void processElement1(AccountBalance snapshot, Context context, Collector<AccountBalance> out) throws Exception {
        processedSnapshots++;

        final String key = String.format("%d-%s-%d", snapshot.getAccountId(), snapshot.getAssetType(), snapshot.getBizId());
        final Long prevSnapshotBlockId = snapshotBlockIdState.value();
        final Long incomingBlockId = snapshot.getBlockId();

        if (prevSnapshotBlockId == null || incomingBlockId >= prevSnapshotBlockId) {
            // 更新基线 & 累计金额/价格
            snapshotBlockIdState.update(incomingBlockId);
            currentAmountState.update(snapshot.getAmount() == null ? BigDecimal.ZERO : snapshot.getAmount());
            lastPriceUsdState.update(snapshot.getPriceUsd() == null ? BigDecimal.ONE : snapshot.getPriceUsd());

            // 先下沉快照（作为该高度的权威余额）
            out.collect(snapshot);

            // 冲刷队列：从当前累计余额开始，按 block_id 递增依次应用 >baseBlock 的增量并下沉
            flushQueuedDeltasAndAccumulate(incomingBlockId, out, snapshot);

            log.debug("📸 Accepted snapshot: key={}, block_id={}, prev_block_id={}", key, incomingBlockId, prevSnapshotBlockId);
        } else {
            droppedSnapshots++;
            log.debug("🗑️ Dropped outdated snapshot: key={}, block_id={}, current_block_id={}", key, incomingBlockId, prevSnapshotBlockId);
        }

        if (processedSnapshots % 100 == 0) {
            double acceptRate = processedSnapshots == 0 ? 0.0 :
                    (double) (processedSnapshots - droppedSnapshots) / (double) processedSnapshots * 100.0;
            log.info("📊 Snapshot stats: processed={}, dropped={}, accept_rate={}%", processedSnapshots, droppedSnapshots,
                    String.format("%.2f", acceptRate));
        }
    }
    
    /**
     * 处理增量流数据  
     * 规则：block_id > 快照流的block_id 才写入
     * 输出：将BalanceDelta转换为AccountBalance格式输出到ClickHouse
     */
    @Override
    public void processElement2(BalanceDelta delta, Context context, Collector<AccountBalance> out) throws Exception {
        processedDeltas++;
        log.info("delta:{}",delta);
        final String key = String.format("%d-%s-%d", delta.getAccountId(), delta.getAssetType(), delta.getBizId());
        final Long snapshotBlockId = snapshotBlockIdState.value();
        final Long deltaBlockId = delta.getBlockId();

        if (snapshotBlockId == null) {
            // 基线未到：进入队列等待
            pendingDeltaQueue.add(delta);
            log.debug("⏸️ Queued delta (waiting snapshot): key={}, block_id={}", key, deltaBlockId);
        } else if (deltaBlockId != null && deltaBlockId > snapshotBlockId) {
            // 基线已就绪：滚动累计余额并下沉
            BigDecimal cur = currentAmountState.value();
            if (cur == null) cur = BigDecimal.ZERO;
            // 优先使用delta中的价格，否则使用最后存储的价格
            BigDecimal px = delta.getPriceUsd() != null ? delta.getPriceUsd() : lastPriceUsdState.value();
            if (px == null) px = BigDecimal.ONE;

            BigDecimal next = cur.add(delta.getDelta() == null ? BigDecimal.ZERO : delta.getDelta());
            currentAmountState.update(next);
            lastPriceUsdState.update(px);

            out.collect(buildBalanceFrom(delta, next, px));
            log.debug("🔄 Applied delta: key={}, block_id={}, snapshot_block_id={}, amount={}→{}", key, deltaBlockId, snapshotBlockId, cur, next);
        } else {
            // 被快照覆盖或无效
            droppedDeltas++;
            log.debug("🗑️ Dropped outdated delta: key={}, block_id={}, snapshot_block_id={}", key, deltaBlockId, snapshotBlockId);
        }

        if (processedDeltas % 100 == 0) {
            double acceptRate = processedDeltas == 0 ? 0.0 :
                    (double) (processedDeltas - droppedDeltas) / (double) processedDeltas * 100.0;
            log.info("📊 Delta stats: processed={}, dropped={}, accept_rate={}%", processedDeltas, droppedDeltas,
                    String.format("%.2f", acceptRate));
        }
    }
    
    /**
     * 按区块升序滚动计算余额，并下沉结果
     */
    private void flushQueuedDeltasAndAccumulate(Long baseBlock, Collector<AccountBalance> out, AccountBalance snapshotCtx) throws Exception {
        if (baseBlock == null) return;

        List<BalanceDelta> buffer = new ArrayList<>();
        for (BalanceDelta d : pendingDeltaQueue.get()) {
            buffer.add(d);
        }
        if (buffer.isEmpty()) return;

        // 升序应用，保证确定性
        Collections.sort(buffer, Comparator.comparingLong(o -> o.getBlockId() == null ? Long.MIN_VALUE : o.getBlockId()));

        BigDecimal cur = currentAmountState.value();
        if (cur == null) cur = BigDecimal.ZERO;
        BigDecimal px = lastPriceUsdState.value();
        if (px == null) px = snapshotCtx.getPriceUsd() == null ? BigDecimal.ONE : snapshotCtx.getPriceUsd();

        for (BalanceDelta d : buffer) {
            Long b = d.getBlockId();
            if (b != null && b > baseBlock) {
                BigDecimal deltaAmt = d.getDelta() == null ? BigDecimal.ZERO : d.getDelta();
                cur = cur.add(deltaAmt);
                // 优先使用增量中的价格
                BigDecimal usePx = d.getPriceUsd() != null ? d.getPriceUsd() : px;
                px = usePx;
                out.collect(buildBalanceFrom(d, cur, usePx));
            }
        }
        // 更新累计余额与最近价格，并清空队列
        currentAmountState.update(cur);
        lastPriceUsdState.update(px);
        pendingDeltaQueue.clear();
    }
    
    /**
     * 根据累计余额构造输出AccountBalance
     */
    private AccountBalance buildBalanceFrom(BalanceDelta delta, BigDecimal amount, BigDecimal px) {
        AccountBalance balance = new AccountBalance();
        balance.setAccountId(delta.getAccountId());
        balance.setObservedTime(delta.getEventTime());
        balance.setBlockId(delta.getBlockId());
        balance.setAssetType(delta.getAssetType());
        balance.setBizId(delta.getBizId());

        balance.setAmount(amount);
        balance.setPriceUsd(px);
        balance.setValueUsd(px.multiply(amount));

        balance.setLabelMask(0);
        balance.setAccountAddress(delta.getAccountAddress());
        balance.setContractAddress(delta.getContractAddress());
        balance.setBizName(delta.getBizName());
        return balance;
    }
    
    /**
     * 创建DualStreamAligner实例
     */
    public static DualStreamAligner create() {
        return new DualStreamAligner();
    }
}
