package com.twilight.aggregator.process.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.AccountMetadata;
import com.twilight.aggregator.model.TokenMetadata;
import com.twilight.aggregator.model.PairMetadata;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import io.lettuce.core.RedisClient;
import com.twilight.aggregator.config.BaseConfig;
public class EventEnrichmentMap extends RichMapFunction<ProcessEvent, ProcessEvent> {

    private static final Logger log = LoggerFactory.getLogger(EventEnrichmentMap.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // —— Redis 同步命令 —— //
    private transient StatefulRedisConnection<String, String> redisConn;
    private transient RedisCommands<String, String> redis;

    // —— 本地只读缓存（启动时一次性加载）—— //
    // Account
    private transient Map<String, AccountMetadata> accountMetadata;  // key: address(lower)
    // Token
    private transient Map<String, TokenMetadata> tokenMetadata;      // key: address(lower)
    // Pair
    private transient Map<String, PairMetadata>  pairMetadata;       // key: pairAddress(lower)

    // —— Redis key（按你现有约定）—— //
    private final String TOKEN_KEY   = "tokenMetadata";
    private final String ACCOUNT_KEY = "accountMetadata";
    private final String PAIR_KEY    = "pairMetadata";

    // —— 统计 —— //
    private long processed = 0L;
    private long enriched  = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 1) 建立 Redis 同步连接（示例使用你已有的封装，按你的项目改）
        RedisSyncConfig cfg = RedisSyncConfig.getInstance(); // 你需要提供这个配置类（或复用现有连接并取 sync()）
        this.redisConn = cfg.getConnection();
        this.redis     = redisConn.sync();

        // 2) 初始化本地缓存容器
        accountMetadata = new ConcurrentHashMap<>();
        tokenMetadata   = new ConcurrentHashMap<>();
        pairMetadata    = new ConcurrentHashMap<>();

        // 3) 启动期一次性加载
        loadAllCachesOnce();

        log.info("✅ SyncEventEnrichmentMap ready. accounts={}, tokens={}, pairs={}",
                accountMetadata.size(), tokenMetadata.size(), pairMetadata.size());
    }

    @Override
    public ProcessEvent map(ProcessEvent e) throws Exception {
        processed++;
        if (e == null) return null;

        // —— 账户增强（fromAddress -> AccountMetadata）——
        if (e.getFromAddress() != null) {
            AccountMetadata am = accountMetadata.get(e.getFromAddress().toLowerCase());
            if (am != null) {
                e.setAccountMetadata(am);
            }
        }
        log.info("EventEnrichmentMap e.getContractAddress() {}",e.getContractAddress());
        log.info("EventEnrichmentMap pairMetadata {}",pairMetadata);
        PairMetadata pm = pairMetadata.get(e.getContractAddress().toLowerCase());
        if(pm != null){
            log.info("EventEnrichmentMap pm {}",pm);
            e.setContractType("dex");
                  e.setPairMetadata(pm);
                  e.setBizId(pm.getPairId());
                  e.setBizName(pm.getPairName());
        }
        else {
            log.info("EventEnrichmentMap pm null");
            // Token 维度（包含 LP-Token 转账场景也可按 token 处理）
            e.setContractType("erc20");
            TokenMetadata tm = tokenMetadata.get(e.getContractAddress().toLowerCase());
            if (tm != null) {
                e.setTokenMetadata(tm);
                e.setBizId(tm.getId());
                e.setBizName(tm.getSymbol());
            }
        }
        enriched++;
        return e;
    }

    @Override
    public void close() throws Exception {
        log.info("🛑 SyncEventEnrichmentMap closed. processed={}, enriched={}", processed, enriched);
        if (redisConn != null) {
            try { redisConn.close(); } catch (Exception ignore) {}
        }
    }

    // ===========================
    // 内部工具
    // ===========================
    private void loadAllCachesOnce() {
        try {
            // 1) Accounts
            String accountJson = redis.get(ACCOUNT_KEY);
            log.info("🔹 accountJson={}", accountJson);
            if (accountJson != null && !accountJson.isEmpty()) {
                List<AccountMetadata> accounts = MAPPER.readValue(accountJson,
                        new TypeReference<List<AccountMetadata>>() {});
                log.info("🔹 accounts={}", accounts);
                for (AccountMetadata a : accounts) {
                    log.info("🔹 a={}", a);
                    if (a != null && a.getAddress() != null) {
                        accountMetadata.put(a.getAddress().toLowerCase(), a);
                    }

                }
                log.info("🔹 accountMetadata={}", accountMetadata);
            }
    
         
            // 3) Tokens
            String tokenJson = redis.get(TOKEN_KEY);
            log.info("🔹 tokenJson={}", tokenJson);
            if (tokenJson != null && !tokenJson.isEmpty()) {
                List<TokenMetadata> tokens = MAPPER.readValue(tokenJson,
                        new TypeReference<List<TokenMetadata>>() {});
                for (TokenMetadata t : tokens) {
                    if (t != null && t.getAddress() != null) {
                        tokenMetadata.put(t.getAddress().toLowerCase(), t);
                    }
                }
            }
       // 2) Pairs
       String pairJson = redis.get(PAIR_KEY);
       log.info("🔹 pairJson={}", pairJson);
       if (pairJson != null && !pairJson.isEmpty()) {
           List<PairMetadata> pairs = MAPPER.readValue(pairJson,
                   new TypeReference<List<PairMetadata>>() {});
           for (PairMetadata p : pairs) {
               if (p != null && p.getPairAddress() != null) {
                   pairMetadata.put(p.getPairAddress().toLowerCase(), p);
                   tryAttachTokenIds(p);
               }
           }
       }

            log.info("✅ Cache loaded. accounts={}, pairs={}, tokens={}",
                    accountMetadata.size(), pairMetadata.size(), tokenMetadata.size());
    
        } catch (Exception ex) {
            log.error("💥 loadAllCachesOnce failed: {}", ex.getMessage(), ex);
        }
    }
    
    private void tryAttachTokenIds(PairMetadata p) {
        try {
            if (p.getToken0() != null && p.getToken0().getId() == null && p.getToken0().getAddress() != null) {
                TokenMetadata t0 = tokenMetadata.get(p.getToken0().getAddress().toLowerCase());
                if (t0 != null) p.getToken0().setId(t0.getId());
            }
            if (p.getToken1() != null && p.getToken1().getId() == null && p.getToken1().getAddress() != null) {
                TokenMetadata t1 = tokenMetadata.get(p.getToken1().getAddress().toLowerCase());
                if (t1 != null) p.getToken1().setId(t1.getId());
            }
        } catch (Exception ignore) {}
    }
 
public static class RedisSyncConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisSyncConfig.class);

    private static volatile RedisSyncConfig INSTANCE;
    private static volatile RedisClient redisClient;
    private static volatile StatefulRedisConnection<String, String> connection;

    private RedisSyncConfig() {
        // 私有化构造，单例模式
    }

    public static RedisSyncConfig getInstance() {
        if (INSTANCE == null) {
            synchronized (RedisSyncConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new RedisSyncConfig();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 构建 Redis URI，从配置文件读取 host/port/password
     * 格式：redis://[:password@]host:port/db
     */
    private String buildRedisUri() {
        String host = BaseConfig.getStaticProperty("redis.host", "localhost");
        String port = BaseConfig.getStaticProperty("redis.port", "6379");
        String password = BaseConfig.getStaticProperty("redis.password", "");
        
        String uri;
        if (password != null && !password.isEmpty()) {
            uri = String.format("redis://:%s@%s:%s/", password, host, port);
        } else {
            uri = String.format("redis://%s:%s/", host, port);
        }
        LOG.info("Redis Lettuce 连接地址: redis://{}:{}/", host, port);
        return uri;
    }

    /**
     * 获取 Lettuce 连接（同步 API）
     */
    public StatefulRedisConnection<String, String> getConnection() {
        if (connection == null || !connection.isOpen()) {
            synchronized (RedisSyncConfig.class) {
                if (connection == null || !connection.isOpen()) {
                    String redisUri = buildRedisUri();
                    redisClient = RedisClient.create(redisUri);
                    connection = redisClient.connect();
                }
            }
        }
        return connection;
    }

    /**
     * 获取同步命令
     */
    public RedisCommands<String, String> getSyncCommands() {
        return getConnection().sync();
    }

    /**
     * 应用关闭时调用，释放资源
     */
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
}
