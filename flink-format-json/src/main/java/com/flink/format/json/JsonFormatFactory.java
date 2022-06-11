package com.flink.format.json;

import com.flink.format.common.TimestampFormat;
import com.flink.format.common.JsonOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.flink.format.common.JsonOptions.FAIL_ON_MISSING_FIELD;
import static com.flink.format.common.JsonOptions.IGNORE_PARSE_ERRORS;
import static com.flink.format.common.JsonOptions.TIMESTAMP_FORMAT;
import static com.flink.format.common.JsonOptions.TIMESTAMP_FORMAT_ENUM;

/**
 * 功能：JsonFormatFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 下午9:36
 */
public class JsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "json";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);

        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);

        return new JsonDecodingFormat(failOnMissingField, ignoreParseErrors, timestampFormat);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        return null;
    }

    // Format 参数验证
    static void validateFormatOptions(ReadableConfig formatOptions) {
        boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        String timestampFormat = formatOptions.get(TIMESTAMP_FORMAT);

        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    String.format("%s and %s shouldn't both be true.",
                            FAIL_ON_MISSING_FIELD.key(), IGNORE_PARSE_ERRORS.key()
                    )
            );
        }
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(
                    String.format("Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].",
                            timestampFormat, TIMESTAMP_FORMAT.key()
                    )
            );
        }
    }
}
