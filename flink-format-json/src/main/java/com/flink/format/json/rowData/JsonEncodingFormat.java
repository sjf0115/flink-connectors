package com.flink.format.json.rowData;

import com.flink.format.json.common.TimestampFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * 功能：Json EncodingFormat
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午2:24
 */
public class JsonEncodingFormat implements EncodingFormat<SerializationSchema<RowData>>{

    private final TimestampFormat timestampFormat;

    public JsonEncodingFormat(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new JsonRowDataSerializationSchema(rowType, timestampFormat);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
