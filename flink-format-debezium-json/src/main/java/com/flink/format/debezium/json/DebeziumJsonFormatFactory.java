package com.flink.format.debezium.json;

import com.flink.format.debezium.common.JsonOptions;
import com.flink.format.debezium.common.TimestampFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
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

import static com.flink.format.debezium.common.JsonOptions.*;

/**
 * 功能：DebeziumJsonFormatFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/11 下午9:36
 */
public class DebeziumJsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "debezium-json";

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
        options.add(SCHEMA_INCLUDE);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final boolean schemaInclude = formatOptions.get(SCHEMA_INCLUDE);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);
        return new DebeziumJsonDecodingFormat(schemaInclude, ignoreParseErrors, timestampFormat);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        throw new UnsupportedOperationException(
                "Debezium format doesn't support as a sink format yet.");
    }
}
