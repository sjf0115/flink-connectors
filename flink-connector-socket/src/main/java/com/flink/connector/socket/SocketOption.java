package com.flink.connector.socket;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * 功能：Socket 参数
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/30 下午10:04
 */
public class SocketOption implements Serializable {
    private String hostname;
    private int port;
    @Nullable
    private String delimiter;
    @Nullable
    private Long maxNumRetries;
    @Nullable
    private Long delayBetweenRetries;

    public SocketOption(String hostname, int port, String delimiter, Long maxNumRetries, Long delayBetweenRetries) {
        this.hostname = hostname;
        this.port = port;
        this.delimiter = delimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public Optional<String> getDelimiter() {
        return Optional.ofNullable(delimiter);
    }

    public Optional<Long> getMaxNumRetries() {
        return Optional.ofNullable(maxNumRetries);
    }

    public Optional<Long> getDelayBetweenRetries() {
        return Optional.ofNullable(delayBetweenRetries);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String hostname;
        private int port;
        private String delimiter;
        private Long maxNumRetries;
        private Long delayBetweenRetries;

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setMaxNumRetries(Long maxNumRetries) {
            this.maxNumRetries = maxNumRetries;
            return this;
        }

        public Builder setDelayBetweenRetries(Long delayBetweenRetries) {
            this.delayBetweenRetries = delayBetweenRetries;
            return this;
        }

        public SocketOption build() {
            return new SocketOption(hostname, port, delimiter, maxNumRetries, delayBetweenRetries);
        }
    }
}
