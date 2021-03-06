package com.flink.connector.socket;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：Socket SourceFunction
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午11:01
 */
public class SocketSourceFunction extends RichSourceFunction<Row> implements ResultTypeQueryable<Row> {
    private static final String DEFAULT_DELIMITER = "\n";
    private static final long DEFAULT_MAX_NUM_RETRIES = 3;
    private static final long DEFAULT_DELAY_BETWEEN_RETRIES = 500;

    private final String hostname;
    private final int port;
    private final String delimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceFunction(SocketOption option, String[] fieldNames, TypeInformation[] fieldTypes) {
        this.hostname = option.getHostname();
        this.port = option.getPort();
        this.delimiter = option.getDelimiter().orElse(DEFAULT_DELIMITER);
        this.maxNumRetries = option.getMaxNumRetries().orElse(DEFAULT_MAX_NUM_RETRIES);
        this.delayBetweenRetries = option.getDelayBetweenRetries().orElse(DEFAULT_DELAY_BETWEEN_RETRIES);
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        long attempt = 0;
        final StringBuilder result = new StringBuilder();
        while (isRunning) {
            try (Socket socket = new Socket()) {
                currentSocket = socket;
                socket.connect(new InetSocketAddress(hostname, port), 0);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    char[] buffer = new char[8012];
                    int bytes;
                    while ((bytes = reader.read(buffer)) != -1) {
                        result.append(buffer, 0, bytes);
                        int delimiterPos;
                        // 根据指定的分隔符循环切分字符串 buffer
                        while (result.length() >= delimiter.length() && (delimiterPos = result.indexOf(delimiter)) != -1) {
                            // 切分字符串 result
                            String record = result.substring(0, delimiterPos);
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            // 输出切分好的字符串
                            sourceContext.collect(Row.of(record));
                            // 切分剩余字符串
                            result.delete(0, delimiterPos + delimiter.length());
                        }
                    }
                }
            }

            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    Thread.sleep(delayBetweenRetries);
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            currentSocket.close();
        } catch (Throwable t) {
            // ignore
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SocketOption socketOption;
        protected String[] fieldNames;
        protected TypeInformation[] fieldTypes;

        public Builder setSocketOption(SocketOption socketOption) {
            this.socketOption = socketOption;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public SocketSourceFunction build() {
            return new SocketSourceFunction(socketOption, fieldNames, fieldTypes);
        }
    }
}
