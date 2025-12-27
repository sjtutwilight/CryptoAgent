// package com.twilight.aggregator.process.pair;

// import java.io.Serializable;
// import java.util.Collection;
// import java.util.Map;
// import java.util.concurrent.atomic.AtomicInteger;

// import org.apache.flink.api.common.state.BroadcastState;
// import org.apache.flink.api.common.state.MapStateDescriptor;
// import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.functions.async.ResultFuture;
// import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
// import org.apache.flink.util.Collector;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.twilight.aggregator.model.Event;
// import com.twilight.aggregator.model.Pair;
// import com.twilight.aggregator.model.PairMetadata;
// import com.twilight.aggregator.source.AsyncPriceLookupFunction;

// public class PairMetadataProcessor extends BroadcastProcessFunction<Event,
// PairMetadata, Pair>
// implements Serializable {
// private static final long serialVersionUID = 1L;
// private final MapStateDescriptor<String, PairMetadata>
// pairMetadataDescriptor;
// private transient Logger log;
// private transient AsyncPriceLookupFunction priceLookup;

// public PairMetadataProcessor(MapStateDescriptor<String, PairMetadata>
// descriptor) {
// this.pairMetadataDescriptor = descriptor;
// }

// @Override
// public void open(Configuration parameters) throws Exception {
// super.open(parameters);
// this.log = LoggerFactory.getLogger(PairMetadataProcessor.class);
// this.priceLookup = new AsyncPriceLookupFunction();
// this.priceLookup.open(parameters);
// }

// @Override
// public void close() throws Exception {
// if (priceLookup != null) {
// priceLookup.close();
// }
// super.close();
// }

// @Override
// public void processElement(Event event, ReadOnlyContext ctx, Collector<Pair>
// out) throws Exception {
// log.info("Processing pair metadata for event: {}", event);
// String eventName = event.getEventName();
// if (isSwapEvent(eventName)) {
// ReadOnlyBroadcastState<String, PairMetadata> broadcastState =
// ctx.getBroadcastState(pairMetadataDescriptor);
// PairMetadata metadata = broadcastState.get(event.getContractAddress());

// if (metadata == null) {
// log.error("Pair metadata not found for contract address: {}",
// event.getContractAddress());
// return;
// }
// log.info("Pair metadata found for contract address: {}", metadata);
// Pair pair = new Pair();
// pair.setPairId(metadata.getPairId());
// pair.setPairAddress(metadata.getPairAddress());
// pair.setToken0Address(metadata.getToken0Address());
// pair.setToken1Address(metadata.getToken1Address());
// pair.setEventName(eventName);

// Map<String, String> decodedArgs = event.getDecodedArgs();
// if (decodedArgs != null) {
// switch (eventName) {
// case "Swap":
// pair.setAmount0In(decodedArgs.get("amount0In"));
// pair.setAmount0Out(decodedArgs.get("amount0Out"));
// pair.setAmount1In(decodedArgs.get("amount1In"));
// pair.setAmount1Out(decodedArgs.get("amount1Out"));
// break;
// case "Sync":
// pair.setReserve0(decodedArgs.get("reserve0"));
// pair.setReserve1(decodedArgs.get("reserve1"));
// break;
// case "Mint":
// case "Burn":
// pair.setSender(decodedArgs.get("sender"));
// pair.setTo(decodedArgs.get("to"));
// pair.setAmount0(decodedArgs.get("amount0"));
// pair.setAmount1(decodedArgs.get("amount1"));
// break;
// }
// }
// log.info("Pair metadata decoded args: {}", decodedArgs);
// pair.setFromAddress(event.getFromAddress());

// // 使用AtomicInteger来跟踪完成的价格查询数量
// AtomicInteger completedQueries = new AtomicInteger(0);

// // Token0 price lookup
// ResultFuture<Double> token0Future = new ResultFuture<Double>() {
// @Override
// public void complete(Collection<Double> result) {
// pair.setToken0PriceUsd(result.iterator().next());
// if (completedQueries.incrementAndGet() == 2) {
// log.info("PairMetadataProcessor processElement: {}", pair);
// out.collect(pair);
// }
// }

// @Override
// public void completeExceptionally(Throwable error) {
// log.error("Error getting token0 price", error);
// pair.setToken0PriceUsd(0.0);
// if (completedQueries.incrementAndGet() == 1) {
// out.collect(pair);
// }
// }
// };

// // Token1 price lookup
// ResultFuture<Double> token1Future = new ResultFuture<Double>() {
// @Override
// public void complete(Collection<Double> result) {
// pair.setToken1PriceUsd(result.iterator().next());
// if (completedQueries.incrementAndGet() == 2) {
// out.collect(pair);
// }
// }

// @Override
// public void completeExceptionally(Throwable error) {
// log.error("Error getting token1 price", error);
// pair.setToken1PriceUsd(0.0);
// if (completedQueries.incrementAndGet() == 1) {
// out.collect(pair);
// }
// }
// };

// // 并行执行两个价格查询
// priceLookup.asyncInvoke(metadata.getToken0Address(), token0Future);
// priceLookup.asyncInvoke(metadata.getToken1Address(), token1Future);
// }
// }

// @Override
// public void processBroadcastElement(PairMetadata metadata, Context ctx,
// Collector<Pair> out) throws Exception {
// BroadcastState<String, PairMetadata> broadcastState =
// ctx.getBroadcastState(pairMetadataDescriptor);
// broadcastState.put(metadata.getPairAddress(), metadata);
// }

// private boolean isSwapEvent(String eventName) {
// return eventName.equals("Swap") ||
// eventName.equals("Sync") ||
// eventName.equals("Mint") ||
// eventName.equals("Burn");
// }
// }