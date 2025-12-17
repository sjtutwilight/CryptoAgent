package com.twilight.metadatacore.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "metadata")
public class MetadataProperties {

    private final Ingestion ingestion = new Ingestion();
    private final Cache cache = new Cache();
    private final Lineage lineage = new Lineage();

    public Ingestion getIngestion() {
        return ingestion;
    }

    public Cache getCache() {
        return cache;
    }

    public Lineage getLineage() {
        return lineage;
    }

    public static class Ingestion {
        /**
         * Kafka topics carrying metadata envelopes.
         */
        private List<String> topics;

        public List<String> getTopics() {
            return topics;
        }

        public void setTopics(List<String> topics) {
            this.topics = topics;
        }
    }

    public static class Cache {
        /**
         * TTL for Redis cache entries.
         */
        private Duration ttl = Duration.ofSeconds(60);

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }
    }

    public static class Lineage {
        /**
         * Maximum depth returned by lineage API.
         */
        private int maxDepth = 3;

        public int getMaxDepth() {
            return maxDepth;
        }

        public void setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
        }
    }
}
