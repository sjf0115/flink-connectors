package com.flink.connector.redis.options;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：Redis 公用参数
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/2 下午5:19
 */
public class RedisOption {
    private String host;
    private int port;
    private boolean clusterMode;
    private String password;
    private int dataBaseNum;
    private String mode;

    private RedisOption(String host, int port, boolean clusterMode, String password, int dataBaseNum, String mode) {
        this.host = host;
        this.port = port;
        this.clusterMode = clusterMode;
        this.password = password;
        this.dataBaseNum = dataBaseNum;
        this.mode = mode;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isClusterMode() {
        return clusterMode;
    }

    public String getPassword() {
        return password;
    }

    public int getDataBaseNum() {
        return dataBaseNum;
    }

    public String getMode() {
        return mode;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private int port;
        private boolean clusterMode;
        private String password;
        private int dataBaseNum;
        private String mode;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setClusterMode(boolean clusterMode) {
            this.clusterMode = clusterMode;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDataBaseNum(int dataBaseNum) {
            this.dataBaseNum = dataBaseNum;
            return this;
        }

        public Builder setMode(String mode) {
            this.mode = mode;
            return this;
        }

        public RedisOption build() {
            checkNotNull(host, "No host supplied.");
            checkNotNull(password, "No password supplied.");
            checkNotNull(mode, "No mode supplied.");
            return new RedisOption(host, port, clusterMode, password, dataBaseNum, mode);
        }
    }
}
