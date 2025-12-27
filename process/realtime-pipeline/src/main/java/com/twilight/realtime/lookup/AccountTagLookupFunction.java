package com.twilight.realtime.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.realtime.model.AccountTag;
import com.twilight.realtime.model.EnrichedSwap;
import com.twilight.realtime.model.OdsDexSwap;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Async lookup for account tags backed by Redis with small L1 cache.
 */
public class AccountTagLookupFunction extends RichAsyncFunction<EnrichedSwap, EnrichedSwap> {
    private static final Logger LOG = LoggerFactory.getLogger(AccountTagLookupFunction.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

    private final String redisHost;
    private final int redisPort;
    private final int redisDatabase;
    private final String redisPassword;
    private final Duration timeout;
    private final long cacheMaxSize;
    private final Duration cacheTtl;

    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String, String> connection;
    private transient RedisAsyncCommands<String, String> asyncCommands;

    private final Map<String, CacheEntry> localCache = new ConcurrentHashMap<>();

    private static class CacheEntry {
        private final AccountTag tag;
        private final long expireAt;

        CacheEntry(AccountTag tag, long expireAt) {
            this.tag = tag;
            this.expireAt = expireAt;
        }
    }

    public AccountTagLookupFunction(String redisHost, int redisPort, int redisDatabase, String redisPassword,
                                    Duration timeout, long cacheMaxSize, Duration cacheTtl) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDatabase = redisDatabase;
        this.redisPassword = redisPassword;
        this.timeout = timeout;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheTtl = cacheTtl;
    }

    @Override
    public void open(Configuration parameters) {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(redisHost)
                .withPort(redisPort)
                .withDatabase(redisDatabase)
                .withTimeout(timeout);
        if (redisPassword != null && !redisPassword.isEmpty()) {
            builder.withPassword(redisPassword.toCharArray());
        }
        redisClient = RedisClient.create(builder.build());
        connection = redisClient.connect();
        asyncCommands = connection.async();
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Override
    public void asyncInvoke(EnrichedSwap input, ResultFuture<EnrichedSwap> resultFuture) {
        OdsDexSwap swap = input.getSwap();
        if (swap == null) {
            resultFuture.complete(Collections.singleton(input));
            return;
        }
        String trader = swap.getTraderAddress();
        if (trader == null || trader.isEmpty()) {
            resultFuture.complete(Collections.singleton(input));
            return;
        }
        String key = cacheKey(swap.getChainId(), trader);
        AccountTag cached = readCache(key);
        if (cached != null) {
            input.setTraderTag(cached);
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        RedisFuture<String> redisFuture = asyncCommands.get(key);
        redisFuture.thenAccept(value -> {
            if (value != null) {
                try {
                    AccountTag tag = parseTag(value);
                    if (tag != null) {
                        input.setTraderTag(tag);
                        writeCache(key, tag);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse account tag for key {}", key, e);
                }
            }
            resultFuture.complete(Collections.singleton(input));
        }).exceptionally(ex -> {
            resultFuture.complete(Collections.singleton(input));
            LOG.error("Account tag lookup failed for key {}", key, ex);
            return null;
        });
    }

    @Override
    public void timeout(EnrichedSwap input, ResultFuture<EnrichedSwap> resultFuture) {
        resultFuture.complete(Collections.singleton(input));
    }

    private String cacheKey(int chainId, String address) {
        return chainId + ":" + address.toLowerCase();
    }

    private AccountTag parseTag(String json) throws Exception {
        JsonNode node = OBJECT_MAPPER.readTree(json);
        AccountTag tag = new AccountTag();
        tag.setChainId(node.path("chain_id").asInt());
        tag.setAccountAddress(node.path("account_address").asText().toLowerCase());
        tag.setWhale(node.path("is_whale").asBoolean(false));
        tag.setSmart(node.path("is_smart").asBoolean(false));
        tag.setBot(node.path("is_bot").asBoolean(false));
        tag.setCexDeposit(node.path("is_cex_deposit").asBoolean(false));
        tag.setVipLevel((short) node.path("vip_level").asInt(0));
        if (!node.path("segment").isMissingNode()) {
            tag.setSegment(node.path("segment").asText());
        }
        if (!node.path("updated_at").isMissingNode()) {
            String ts = node.path("updated_at").asText();
            tag.setUpdatedAt(Instant.parse(ts));
        }
        return tag;
    }

    private AccountTag readCache(String key) {
        CacheEntry entry = localCache.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.expireAt < System.currentTimeMillis()) {
            localCache.remove(key);
            return null;
        }
        return entry.tag;
    }

    private void writeCache(String key, AccountTag tag) {
        if (localCache.size() >= cacheMaxSize) {
            localCache.clear();
        }
        long expireAt = System.currentTimeMillis() + cacheTtl.toMillis();
        localCache.put(key, new CacheEntry(tag, expireAt));
    }
}
