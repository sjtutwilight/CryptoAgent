package com.twilight.realtime.process;

import com.twilight.realtime.model.EnrichedSwap;
import com.twilight.realtime.model.OdsDexSwap;
import com.twilight.realtime.model.TokenPrice;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Maintains keyed token price state and enriches swaps without broadcasting high frequency updates.
 */
public class PriceStateEnrichmentFunction extends KeyedCoProcessFunction<Integer, EnrichedSwap, TokenPrice, EnrichedSwap> {

    private final long maxPriceAgeMs;
    private final String nativeTokenAddress;
    private transient MapState<String, TokenPrice> priceState;

    public PriceStateEnrichmentFunction(long maxPriceAgeMs, String nativeTokenAddress) {
        this.maxPriceAgeMs = Math.max(maxPriceAgeMs, 0);
        this.nativeTokenAddress = nativeTokenAddress == null ? null : nativeTokenAddress.toLowerCase();
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TokenPrice> descriptor = new MapStateDescriptor<>(
                "tokenPriceState",
                TypeInformation.of(String.class),
                TypeInformation.of(TokenPrice.class));
        priceState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement1(EnrichedSwap value, Context ctx, Collector<EnrichedSwap> out) throws Exception {
        if (value == null || value.getSwap() == null) {
            return;
        }
        OdsDexSwap swap = value.getSwap();
        value.setToken0Price(lookupPrice(swap.getToken0Address()));
        value.setToken1Price(lookupPrice(swap.getToken1Address()));
        if (nativeTokenAddress != null) {
            value.setNativeTokenPrice(lookupPrice(nativeTokenAddress));
        }
        out.collect(value);
    }

    @Override
    public void processElement2(TokenPrice price, Context ctx, Collector<EnrichedSwap> out) throws Exception {
        if (price == null || price.getTokenAddress() == null) {
            return;
        }
        priceState.put(normalize(price.getTokenAddress()), price);
    }

    private TokenPrice lookupPrice(String tokenAddress) throws Exception {
        if (tokenAddress == null || tokenAddress.isEmpty()) {
            return null;
        }
        TokenPrice price = priceState.get(normalize(tokenAddress));
        if (!isFresh(price)) {
            return null;
        }
        return price;
    }

    private boolean isFresh(TokenPrice price) {
        if (price == null) {
            return false;
        }
        if (maxPriceAgeMs <= 0 || price.getUpdatedAt() == null) {
            return true;
        }
        long age = Instant.now().toEpochMilli() - price.getUpdatedAt().toEpochMilli();
        return age <= maxPriceAgeMs;
    }

    private static String normalize(String value) {
        return value == null ? null : value.toLowerCase();
    }
}
