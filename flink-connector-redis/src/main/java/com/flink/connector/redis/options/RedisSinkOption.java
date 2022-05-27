package com.flink.connector.redis.options;

/**
 * 功能：Redis Sink 参数
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/2 下午5:23
 */
public class RedisSinkOption {
    private int keyExpire;
    private boolean ignoreDelete;

    private RedisSinkOption(int keyExpire, boolean ignoreDelete) {
        this.keyExpire = keyExpire;
        this.ignoreDelete = ignoreDelete;
    }

    public int getKeyExpire() {
        return keyExpire;
    }

    public boolean isIgnoreDelete() {
        return ignoreDelete;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int keyExpire;
        private boolean ignoreDelete;

        public Builder setKeyExpire(int keyExpire) {
            this.keyExpire = keyExpire;
            return this;
        }

        public Builder setIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public RedisSinkOption build() {
            return new RedisSinkOption(keyExpire, ignoreDelete);
        }
    }
}
