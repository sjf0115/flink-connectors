package com.flink.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 功能：ChangelogCsvDeserializer
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 上午8:34
 */
public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {
    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final String columnDelimiter;

    public ChangelogCsvDeserializer(
            List<LogicalType> parsingTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo,
            String columnDelimiter) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public void open(InitializationContext context) {
        // converters must be open
        converter.open(RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        // parse the columns including a changelog flag
        String message = new String(bytes);
        final String[] columns = message.split(Pattern.quote(columnDelimiter));
        final RowKind kind = RowKind.valueOf(columns[0]);
        final Row row = new Row(kind, parsingTypes.size());
        for (int i = 0; i < parsingTypes.size(); i++) {
            LogicalTypeRoot type = parsingTypes.get(i).getTypeRoot();
            String value = columns[i + 1];
            Object object = parse(type, value);
            row.setField(i, object);
        }
        // convert to internal data structure
        return (RowData) converter.toInternal(row);
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }
}
