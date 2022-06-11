package com.flink.format.json;

import com.flink.format.common.TimestampFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：Json RowData DeserializationSchema
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/8 上午8:36
 */
public class JsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final boolean failOnMissingField;
    private final boolean ignoreParseErrors;
    private final TypeInformation<RowData> resultTypeInfo;
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TimestampFormat timestampFormat;

    public JsonRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo,
                                            boolean failOnMissingField, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }

        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter = new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                .createConverter(checkNotNull(rowType));
        this.timestampFormat = timestampFormat;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            final JsonNode root = objectMapper.readTree(message);
            return (RowData) runtimeConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonRowDataDeserializationSchema that = (JsonRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField
                && ignoreParseErrors == that.ignoreParseErrors
                && resultTypeInfo.equals(that.resultTypeInfo)
                && timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat);
    }
}
