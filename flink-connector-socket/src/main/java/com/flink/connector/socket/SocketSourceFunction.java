package com.flink.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：Socket SourceFunction
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午11:01
 */
public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private static final int DEFAULT_DELIMITER = 10;
    private static final long DEFAULT_MAX_NUM_RETRIES = 3;
    private static final long DEFAULT_DELAY_BETWEEN_RETRIES = 500;

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;
    private final DeserializationSchema<RowData> deserializer;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceFunction(SocketOption option, DeserializationSchema<RowData> deserializer) {
        this.hostname = option.getHostname();
        this.port = option.getPort();
        this.byteDelimiter = (byte)(int)option.getByteDelimiter().orElse(DEFAULT_DELIMITER);
        this.maxNumRetries = option.getMaxNumRetries().orElse(DEFAULT_MAX_NUM_RETRIES);
        this.delayBetweenRetries = option.getDelayBetweenRetries().orElse(DEFAULT_DELAY_BETWEEN_RETRIES);
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (isRunning) {
            // open and consume from socket
            try (final Socket socket = new Socket()) {
                currentSocket = socket;
                socket.connect(new InetSocketAddress(hostname, port), 0);
                try (InputStream stream = socket.getInputStream()) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int b;
                    while ((b = stream.read()) >= 0) {
                        // buffer until delimiter
                        if (b != byteDelimiter) {
                            buffer.write(b);
                        }
                        // decode and emit record
                        else {
                            sourceContext.collect(deserializer.deserialize(buffer.toByteArray()));
                            buffer.reset();
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace(); // print and continue
            }
            Thread.sleep(1000);
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
        private DeserializationSchema<RowData> deserializer;

        public Builder setSocketOption(SocketOption socketOption) {
            this.socketOption = socketOption;
            return this;
        }

        public Builder setDeserializationSchema(DeserializationSchema<RowData> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SocketSourceFunction build() {
            return new SocketSourceFunction(socketOption, deserializer);
        }
    }
}
