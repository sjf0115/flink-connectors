package com.flink.format.changelog.csv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * 功能：ChangelogCsvDeserializationSchema
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 上午8:34
 */
public class ChangelogCsvDeserializationSchema implements DeserializationSchema<RowData> {
    private final TypeInformation<RowData> resultTypeInfo;
    private final String columnDelimiter;
    private final ChangelogCsvToRowDataConverter converter;

    public ChangelogCsvDeserializationSchema(RowType rowType, TypeInformation<RowData> producedTypeInfo, String columnDelimiter) {
        this.resultTypeInfo = producedTypeInfo;
        this.columnDelimiter = columnDelimiter;
        this.converter = new ChangelogCsvToRowDataConverter(rowType);
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        String message = new String(bytes);
        final String[] columns = message.split(Pattern.quote(columnDelimiter));
        // String 转 RowData
        return converter.convert(columns);
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
}
