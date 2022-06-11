package com.flink.format.json.rowData;

import com.flink.format.json.common.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

import static java.time.format.DateTimeFormatter.*;
import static com.flink.format.json.common.TimeFormats.*;

/**
 * 功能：转换器 RowData 转换 Json
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午2:31
 */
public class RowDataToJsonConverters implements Serializable {
    private final TimestampFormat timestampFormat;

    public RowDataToJsonConverters(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public interface RowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }

    public RowDataToJsonConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private RowDataToJsonConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
            case BOOLEAN:
                return (mapper, reuse, value) ->
                        mapper.getNodeFactory().booleanNode((boolean) value);
            case TINYINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
            case SMALLINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
            case FLOAT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
            case DOUBLE:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
            case BINARY:
            case VARBINARY:
                return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
            case DATE:
                return createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case DECIMAL:
                return createDecimalConverter();
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
            case MULTISET:
                return createMapConverter((MapType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return mapper.getNodeFactory().numberNode(bd);
        };
    }

    private RowDataToJsonConverter createDateConverter() {
        return (mapper, reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
        };
    }

    private RowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return mapper.getNodeFactory().textNode(SQL_TIME_FORMAT.format(time));
        };
    }

    private RowDataToJsonConverter createTimestampConverter() {
        switch (timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData) value;
                    return mapper.getNodeFactory()
                            .textNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToJsonConverter createArrayConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        final RowDataToJsonConverter elementConverter = createConverter(elementType);
        return (mapper, reuse, value) -> {
            ArrayNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createArrayNode();
            } else {
                node = (ArrayNode) reuse;
                node.removeAll();
            }

            ArrayData array = (ArrayData) value;
            int numElements = array.size();
            for (int i = 0; i < numElements; i++) {
                Object element = ArrayData.get(array, i, elementType);
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createMapConverter(MapType type) {
        LogicalType keyType = type.getKeyType();
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The map type is: "
                            + type.asSummaryString());
        }
        final LogicalType valueType = type.getValueType();
        final RowDataToJsonConverter valueConverter = createConverter(valueType);
        return (mapper, reuse, object) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
                node.removeAll();
            }

            MapData map = (MapData) object;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            int numElements = map.size();
            for (int i = 0; i < numElements; i++) {
                String fieldName = keyArray.getString(i).toString(); // key must be string
                Object value = ArrayData.get(valueArray, i, valueType);
                node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createRowConverter(RowType type) {
        final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowDataToJsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToJsonConverter[]::new);
        final int fieldCount = type.getFieldCount();

        return (mapper, reuse, value) -> {
            ObjectNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
            }
            RowData row = (RowData) value;
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames[i];
                Object field = RowData.get(row, i, fieldTypes[i]);
                node.set(fieldName, fieldConverters[i].convert(mapper, node.get(fieldName), field));
            }
            return node;
        };
    }

    private RowDataToJsonConverter wrapIntoNullableConverter(RowDataToJsonConverter converter) {
        return (mapper, reuse, object) -> {
            if (object == null) {
                return mapper.getNodeFactory().nullNode();
            }

            return converter.convert(mapper, reuse, object);
        };
    }
}
