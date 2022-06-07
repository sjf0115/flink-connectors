package com.flink.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;


/**
 * 功能：Socket Source TableFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:40
 */
public class SocketSourceTableFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> DELIMITER = ConfigOptions.key("byte-delimiter")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Long> MAX_NUM_RETRIES = ConfigOptions.key("max_num_retries")
            .longType()
            .defaultValue(3L);

    public static final ConfigOption<Long> DELAY_BETWEEN_RETRIES = ConfigOptions.key("delay_between_retries")
            .longType()
            .defaultValue(500L);

    @Override
    public String factoryIdentifier() {
        return "socket";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DELIMITER);
        options.add(MAX_NUM_RETRIES);
        options.add(DELAY_BETWEEN_RETRIES);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);

        SocketOption.Builder builder = SocketOption.builder()
                .setHostname(hostname)
                .setPort(port);

        options.getOptional(DELIMITER).ifPresent(builder::setByteDelimiter);
        options.getOptional(MAX_NUM_RETRIES).ifPresent(builder::setMaxNumRetries);
        options.getOptional(DELAY_BETWEEN_RETRIES).ifPresent(builder::setDelayBetweenRetries);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        // 创建 SocketTableSource
        SocketDynamicTableSource tableSource = SocketDynamicTableSource.builder()
                .setSocketOption(builder.build())
                .setDecodingFormat(decodingFormat)
                .setProducedDataType(producedDataType)
                .build();
        return tableSource;
    }
}
