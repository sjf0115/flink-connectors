package com.flink.connector.redis.options;

/**
 * 功能：RedisLookupOption
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/2 下午10:51
 */
public class RedisLookupOption {

    // 缓存策略
    private String cache;
    // 缓存大小
    private long cacheSize;
    // 缓存超时时长
    private long cacheTTLMs;
    // 是否缓存空结果
    private boolean cacheEmpty;

    private RedisLookupOption(String cache, long cacheSize, long cacheTTLMs, boolean cacheEmpty) {
        this.cache = cache;
        this.cacheSize = cacheSize;
        this.cacheTTLMs = cacheTTLMs;
        this.cacheEmpty = cacheEmpty;
    }

    public String getCache() {
        return cache;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public long getCacheTTLMs() {
        return cacheTTLMs;
    }

    public boolean isCacheEmpty() {
        return cacheEmpty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String cache;
        private long cacheSize;
        private long cacheTTLMs;
        private boolean cacheEmpty;

        public Builder setCache(String cache) {
            this.cache = cache;
            return this;
        }

        public Builder setCacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder setCacheTTLMs(long cacheTTLMs) {
            this.cacheTTLMs = cacheTTLMs;
            return this;
        }

        public Builder setCacheEmpty(boolean cacheEmpty) {
            this.cacheEmpty = cacheEmpty;
            return this;
        }

        public RedisLookupOption build() {
            return new RedisLookupOption(cache, cacheSize, cacheTTLMs, cacheEmpty);
        }
    }

}
