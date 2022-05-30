package com.flink.connector.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * 功能：Socket TableSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:57
 */
public class SocketTableSource implements StreamTableSource<String>{
    private final SocketOption socketOption;
    private final TableSchema schema;

    public SocketTableSource(SocketOption socketOption, TableSchema schema) {
        this.socketOption = socketOption;
        this.schema = schema;
    }

    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        SocketSourceFunction socketSourceFunction = new SocketSourceFunction(socketOption);
        return env.addSource(socketSourceFunction);
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TableSchema schema;
        private SocketOption socketOption;

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setSocketOption(SocketOption socketOption) {
            this.socketOption = socketOption;
            return this;
        }

        public SocketTableSource build() {
            return new SocketTableSource(socketOption, schema);
        }
    }
}
