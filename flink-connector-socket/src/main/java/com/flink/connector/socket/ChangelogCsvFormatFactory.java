package com.flink.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 功能：ChangelogCsvFormatFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/7 上午8:57
 */
public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

    public static final ConfigOption<String> COLUMN_DELIMITER = ConfigOptions.key("column-delimiter")
            .stringType()
            .defaultValue("|");

    @Override
    public String factoryIdentifier() {
        return "changelog-csv";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(COLUMN_DELIMITER);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        // get the validated options
        final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);

        // create and return the format
        return new ChangelogCsvFormat(columnDelimiter);
    }
}
