package com.flink.format.json;

import com.flink.format.common.TimeFormats;
import com.flink.format.common.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 功能：转换器 Json 转换 RowData
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/8 上午8:53
 */
public class JsonToRowDataConverters implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean failOnMissingField;
    private boolean ignoreParseErrors;
    private final TimestampFormat timestampFormat;

    public JsonToRowDataConverters(boolean failOnMissingField, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @FunctionalInterface
    public interface JsonToRowDataConverter extends Serializable {
        Object convert(JsonNode jsonNode);
    }

    // 创建运行时 Converter
    public JsonToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private JsonToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return jsonNode -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
            case SMALLINT:
                return jsonNode -> Short.parseShort(jsonNode.asText().trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
            case MULTISET:
                return createMapConverter((MapType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private JsonToRowDataConverter wrapIntoNullableConverter(
            JsonToRowDataConverter converter) {
        return jsonNode -> {
            if (jsonNode == null || jsonNode.isNull()) {
                return null;
            }
            try {
                return converter.convert(jsonNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    public JsonToRowDataConverter createRowConverter(RowType rowType) {
        final JsonToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(JsonToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return jsonNode -> {
            ObjectNode node = (ObjectNode) jsonNode;
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                JsonNode field = node.get(fieldName);
                Object convertedField = convertField(fieldConverters[i], fieldName, field);
                row.setField(i, convertedField);
            }
            return row;
        };
    }

    private Object convertField(JsonToRowDataConverter fieldConverter, String fieldName, JsonNode field) {
        if (field == null) {
            if (failOnMissingField) {
                throw new JsonParseException("Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(field);
        }
    }

    private boolean convertToBoolean(JsonNode jsonNode) {
        if (jsonNode.isBoolean()) {
            return jsonNode.asBoolean();
        } else {
            return Boolean.parseBoolean(jsonNode.asText().trim());
        }
    }

    private int convertToInt(JsonNode jsonNode) {
        if (jsonNode.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return jsonNode.asInt();
        } else {
            return Integer.parseInt(jsonNode.asText().trim());
        }
    }

    private long convertToLong(JsonNode jsonNode) {
        if (jsonNode.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return jsonNode.asLong();
        } else {
            return Long.parseLong(jsonNode.asText().trim());
        }
    }

    private double convertToDouble(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return jsonNode.asDouble();
        } else {
            return Double.parseDouble(jsonNode.asText().trim());
        }
    }

    private float convertToFloat(JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return (float) jsonNode.asDouble();
        } else {
            return Float.parseFloat(jsonNode.asText().trim());
        }
    }

    private int convertToDate(JsonNode jsonNode) {
        LocalDate date = DateTimeFormatter.ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private int convertToTime(JsonNode jsonNode) {
        TemporalAccessor parsedTime = TimeFormats.SQL_TIME_FORMAT.parse(jsonNode.asText());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(JsonNode jsonNode) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = TimeFormats.SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
                break;
            case ISO_8601:
                parsedTimestamp = TimeFormats.ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private StringData convertToString(JsonNode jsonNode) {
        return StringData.fromString(jsonNode.asText());
    }

    private byte[] convertToBytes(JsonNode jsonNode) {
        try {
            return jsonNode.binaryValue();
        } catch (IOException e) {
            throw new JsonParseException("Unable to deserialize byte array.", e);
        }
    }

    private JsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return jsonNode -> {
            BigDecimal bigDecimal;
            if (jsonNode.isBigDecimal()) {
                bigDecimal = jsonNode.decimalValue();
            } else {
                bigDecimal = new BigDecimal(jsonNode.asText());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private JsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        JsonToRowDataConverter elementConverter =
                createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return jsonNode -> {
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }
            return new GenericArrayData(array);
        };
    }

    private JsonToRowDataConverter createMapConverter(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The map type is: "
                            + mapType.asSummaryString());
        }
        final JsonToRowDataConverter keyConverter = createConverter(keyType);
        final JsonToRowDataConverter valueConverter =
                createConverter(mapType.getValueType());

        return jsonNode -> {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    // 解析异常
    private static final class JsonParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public JsonParseException(String message) {
            super(message);
        }
        public JsonParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
