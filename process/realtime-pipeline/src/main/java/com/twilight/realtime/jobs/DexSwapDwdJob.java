package com.twilight.realtime.jobs;

import com.twilight.realtime.config.PipelineConfig;
import com.twilight.realtime.lookup.AccountTagLookupFunction;
import com.twilight.realtime.model.DwdDexSwap;
import com.twilight.realtime.model.EnrichedSwap;
import com.twilight.realtime.model.OdsDexSwap;
import com.twilight.realtime.model.TokenPrice;
import com.twilight.realtime.process.DwdProjectionMapFunction;
import com.twilight.realtime.process.PriceStateEnrichmentFunction;
import com.twilight.realtime.serialization.JsonDeserializationSchema;
import com.twilight.realtime.serialization.JsonSerializationSchema;
import com.twilight.realtime.util.KeyUtil;
import com.twilight.realtime.util.TopicNames;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Main Flink job that enriches dex swap facts with account tags and prices.
 */
public class DexSwapDwdJob {
    public static void main(String[] args) throws Exception {
        PipelineConfig config = PipelineConfig.load();
        int chainId = config.getChainId();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointIntervalMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getCheckpointMinPauseMs());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(10)));

        WatermarkStrategy<OdsDexSwap> swapWatermark = timestampStrategy(
                config.getMaxOutOfOrdernessSeconds(),
                (event, timestamp) -> event != null && event.getBlockTimestamp() != null
                        ? event.getBlockTimestamp().toEpochMilli()
                        : System.currentTimeMillis());

        String perChainDefault = chainId == 42161
                ? config.getTopic(TopicNames.ODS_SWAP_FULL_ARB, "ods_dex_swap_full_arb")
                : config.getTopic(TopicNames.ODS_SWAP_FULL_ETH, "ods_dex_swap_full_eth");
        String swapTopic = config.getTopic(TopicNames.ODS_SWAP_FULL, perChainDefault);

        DataStream<OdsDexSwap> rawSwapStream = env.fromSource(
                        buildSingleTopicSource(config, swapTopic, new JsonDeserializationSchema<>(OdsDexSwap.class)),
                        swapWatermark,
                        "ods_swap_full_source")
                .name("ODS Swap Full Source");

        DataStream<OdsDexSwap> filteredSwapStream = rawSwapStream
                .filter(swap -> swap != null && swap.getChainId() == chainId)
                .name("Chain Filter");

        DataStream<EnrichedSwap> baseSwaps = filteredSwapStream
                .map(swap -> {
                    EnrichedSwap enriched = new EnrichedSwap();
                    enriched.setSwap(swap);
                    return enriched;
                })
                .returns(EnrichedSwap.class)
                .name("Wrap Swap");

        RichAsyncFunction<EnrichedSwap, EnrichedSwap> asyncLookup = new AccountTagLookupFunction(
                config.getRedisHost(),
                config.getRedisPort(),
                config.getRedisDatabase(),
                config.getRedisPassword(),
                Duration.ofMillis(config.getRedisTimeoutMs()),
                config.getRedisCacheMaxSize(),
                Duration.ofSeconds(config.getRedisCacheTtlSeconds()));

        DataStream<EnrichedSwap> withTags = AsyncDataStream.unorderedWait(
                        baseSwaps,
                        asyncLookup,
                        config.getRedisTimeoutMs(),
                        TimeUnit.MILLISECONDS,
                        1000)
                .name("Account Tag Lookup");

        WatermarkStrategy<TokenPrice> priceWatermark = timestampStrategy(
                config.getMaxOutOfOrdernessSeconds(),
                (event, timestamp) -> event != null && event.getUpdatedAt() != null
                        ? event.getUpdatedAt().toEpochMilli()
                        : System.currentTimeMillis());

        DataStream<TokenPrice> priceStream = env.fromSource(
                        buildSingleTopicSource(config,
                                config.getTopic(TopicNames.DIM_PRICE, "dim_token_price_current"),
                                new JsonDeserializationSchema<>(TokenPrice.class)),
                        priceWatermark,
                        "dim_price_source")
                .name("DIM Price Source")
                .filter(price -> price != null && price.getChainId() == chainId)
                .name("Price Chain Filter");

        KeyedStream<EnrichedSwap, Integer> keyedSwaps = withTags
                .keyBy(value -> value.getSwap().getChainId());
        KeyedStream<TokenPrice, Integer> keyedPrices = priceStream
                .keyBy(TokenPrice::getChainId);

        DataStream<EnrichedSwap> fullyEnriched = keyedSwaps
                .connect(keyedPrices)
                .process(new PriceStateEnrichmentFunction(
                        config.getPriceLookupMaxPastMillis(),
                        config.getNativeTokenAddress()))
                .name("Price State Enrich");

        DataStream<DwdDexSwap> dwdStream = fullyEnriched
                .map(new DwdProjectionMapFunction())
                .name("Project to DWD");

        String dwdTopic = config.getTopic(TopicNames.DWD_SWAP, "dwd_dex_swap");
        KafkaSink<DwdDexSwap> dwdSink = KafkaSink.<DwdDexSwap>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<DwdDexSwap>builder()
                                .setTopic(dwdTopic)
                                .setKeySerializationSchema(element -> KeyUtil.swapKey(
                                        element.getChainId(),
                                        element.getTxHash(),
                                        element.getLogIndex()).getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dwdStream.sinkTo(dwdSink).name("DWD Kafka Sink");

        env.execute("dex_swap_dwd_job_chain_" + chainId);
    }

    private static <T> KafkaSource<T> buildSingleTopicSource(PipelineConfig config,
                                                             String topic,
                                                             JsonDeserializationSchema<T> schema) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setGroupId(config.getKafkaGroupId() + "-" + topic)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(schema)
                .build();
    }

    private static <T> WatermarkStrategy<T> timestampStrategy(long maxOutOfOrderSeconds,
                                                              SerializableTimestampAssigner<T> assigner) {
        return WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderSeconds))
                .withTimestampAssigner(assigner)
                .withIdleness(Duration.ofMinutes(2));
    }
}
