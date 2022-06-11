package com.flink.format.changelog.csv;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.List;

/**
 * 功能：ChangelogCsvToRowDataConverters
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午3:43
 */
public class ChangelogCsvToRowDataConverter implements Serializable{
    private final RowType rowType;

    public ChangelogCsvToRowDataConverter(RowType rowType) {
        this.rowType = rowType;
    }

    public RowData convert(String[] columns) {
        // 第一列为 RowKind
        final RowKind kind = RowKind.valueOf(columns[0]);
        // 其余列为数据列
        List<RowType.RowField> fields = rowType.getFields();
        int size = fields.size();
        GenericRowData row = new GenericRowData(kind, size);
        for (int i = 0; i < size; i++) {
            LogicalType type = fields.get(i).getType();
            String value = columns[i + 1];
            Object object = parse(type, value);
            row.setField(i, object);
        }
        return row;
    }

    private static Object parse(LogicalType type, String value) {
        switch (type.getTypeRoot()) {
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case FLOAT:
                return Float.parseFloat(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case VARCHAR:
                return StringData.fromString(value);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
