package com.flink.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * 功能：Socket TableSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:57
 */
public class SocketDynamicTableSource implements ScanTableSource {
    private final SocketOption socketOption;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public SocketDynamicTableSource(SocketOption socketOption, DecodingFormat format, DataType producedDataType) {
        this.socketOption = socketOption;
        this.decodingFormat = format;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // create runtime classes that are shipped to the cluster
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);

        final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(socketOption, deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(socketOption, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Socket Dynamic Table Source";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private SocketOption socketOption;
        private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
        private DataType producedDataType;

        public Builder setDecodingFormat(DecodingFormat decodingFormat) {
            this.decodingFormat = decodingFormat;
            return this;
        }

        public Builder setSocketOption(SocketOption socketOption) {
            this.socketOption = socketOption;
            return this;
        }

        public Builder setProducedDataType(DataType producedDataType) {
            this.producedDataType = producedDataType;
            return this;
        }

        public SocketDynamicTableSource build() {
            return new SocketDynamicTableSource(socketOption, decodingFormat, producedDataType);
        }
    }
}
