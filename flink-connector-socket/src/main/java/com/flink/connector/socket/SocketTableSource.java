package com.flink.connector.socket;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

/**
 * 功能：Socket TableSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:57
 */
public class SocketTableSource implements StreamTableSource<Row>{
    private final SocketOption socketOption;
    private final TableSchema schema;
    private final DataType producedDataType;

    public SocketTableSource(SocketOption socketOption, TableSchema schema) {
        this.socketOption = socketOption;
        this.schema = schema;
        this.producedDataType = schema.toRowDataType();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
        SocketSourceFunction socketSourceFunction = SocketSourceFunction.builder()
                .setSocketOption(socketOption)
                .setFieldNames(rowTypeInfo.getFieldNames())
                .setFieldTypes(rowTypeInfo.getFieldTypes())
                .build();
        return env.addSource(socketSourceFunction);
    }

    @Override
    public DataType getProducedDataType() {
        return producedDataType;
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
