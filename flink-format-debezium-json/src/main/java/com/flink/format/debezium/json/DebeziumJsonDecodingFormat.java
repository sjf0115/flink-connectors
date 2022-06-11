package com.flink.format.debezium.json;

import com.flink.format.debezium.common.TimestampFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * 功能：DebeziumJsonDecodingFormat
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午9:51
 */
public class DebeziumJsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final boolean schemaInclude;
    private final boolean ignoreParseErrors;
    private TimestampFormat timestampFormat;

    public DebeziumJsonDecodingFormat(boolean schemaInclude, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        this.schemaInclude = schemaInclude;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
                (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
        return new DebeziumJsonDeserializationSchema(
                rowType,
                rowDataTypeInfo,
                schemaInclude,
                ignoreParseErrors,
                timestampFormat);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
