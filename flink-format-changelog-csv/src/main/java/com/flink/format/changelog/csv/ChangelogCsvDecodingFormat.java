package com.flink.format.changelog.csv;

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
 * 功能：ChangelogCsvDecodingFormat
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 上午8:59
 */
public class ChangelogCsvDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String columnDelimiter;

    public ChangelogCsvDecodingFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> resultTypeInfo =
                (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
        return new ChangelogCsvDeserializationSchema(rowType, resultTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // 支持 INSERT 和 DELETE 模式
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
