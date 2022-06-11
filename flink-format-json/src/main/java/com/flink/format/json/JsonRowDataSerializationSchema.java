package com.flink.format.json;

import com.flink.format.common.TimestampFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * 功能：JsonRowDataSerializationSchema
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午2:28
 */
public class JsonRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final RowType rowType;
    private final TimestampFormat timestampFormat;
    private final RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter;

    private transient ObjectNode node;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonRowDataSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
        this.rowType = rowType;
        this.timestampFormat = timestampFormat;
        runtimeConverter = new RowDataToJsonConverters(timestampFormat).createConverter(rowType);
    }

    @Override
    public byte[] serialize(RowData rowData) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, rowData);
            return mapper.writeValueAsBytes(node);
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + rowData + "'. ", t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonRowDataSerializationSchema that = (JsonRowDataSerializationSchema) o;
        return rowType.equals(that.rowType) && timestampFormat.equals(timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, timestampFormat);
    }
}
