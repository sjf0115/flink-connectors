package com.flink.format.json.rowData;

import com.flink.format.json.common.TimestampFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * 功能：JsonDecodingFormat
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 下午9:42
 */
public class JsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>>{
    private final boolean failOnMissingField;
    private final boolean ignoreParseErrors;
    private final TimestampFormat timestampFormat;

    public JsonDecodingFormat(boolean failOnMissingField, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
                (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
        return new JsonRowDataDeserializationSchema(
                rowType,
                rowDataTypeInfo,
                failOnMissingField,
                ignoreParseErrors,
                timestampFormat);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
