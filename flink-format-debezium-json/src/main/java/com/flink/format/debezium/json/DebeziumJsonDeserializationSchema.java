package com.flink.format.debezium.json;

import com.flink.format.debezium.common.TimestampFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * 功能：DebeziumJsonDeserializationSchema
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午9:56
 */
public class DebeziumJsonDeserializationSchema implements DeserializationSchema<RowData> {

    private final boolean schemaInclude;
    private final boolean ignoreParseErrors;
    private TimestampFormat timestampFormat;
    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;


    public DebeziumJsonDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo,
              boolean schemaInclude, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        this.schemaInclude = schemaInclude;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
