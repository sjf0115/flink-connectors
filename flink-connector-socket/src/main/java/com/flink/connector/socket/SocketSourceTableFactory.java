package com.flink.connector.socket;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flink.connector.socket.SocketValidator.*;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.*;


/**
 * 功能：Socket Source TableFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:40
 */
public class SocketSourceTableFactory implements StreamTableSourceFactory {

    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
        return context;
    }

    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // option
        properties.add(CONNECTOR_HOST);
        properties.add(CONNECTOR_PORT);
        properties.add(CONNECTOR_DELIMITER);
        properties.add(CONNECTOR_MAX_NUM_RETRIES);
        properties.add(CONNECTOR_DELAY_BETWEEN_RETRIES);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // format wildcard
        properties.add(FORMAT + ".*");
        properties.add(CONNECTOR + ".*");
        return properties;
    }

    public StreamTableSource createStreamTableSource(Map properties) {
        // 有效性校验
        DescriptorProperties validatedProperties = getValidatedProperties(properties);

        // Socket 参数
        SocketOption socketOption = getSocketOption(validatedProperties);

        // TableSchema
        TableSchema tableSchema = validatedProperties.getTableSchema(SCHEMA);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(tableSchema);

        // 创建 SocketTableSource
        SocketTableSource tableSource = SocketTableSource.builder()
                .setSchema(schema)
                .setSocketOption(socketOption)
                .build();
        return tableSource;
    }

    // 有效 Properties
    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        // Map -> DescriptorProperties
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // Schema 校验
        SchemaValidator schemaValidator = new SchemaValidator(true, false, false);
        schemaValidator.validate(descriptorProperties);

        // Socket 参数校验
        SocketValidator socketValidator = new SocketValidator();
        socketValidator.validate(descriptorProperties);

        return descriptorProperties;
    }

    // Socket 参数
    private SocketOption getSocketOption(DescriptorProperties descriptorProperties) {
        String host = descriptorProperties.getString(CONNECTOR_HOST);
        int port = descriptorProperties.getInt(CONNECTOR_PORT);

        SocketOption.Builder builder = SocketOption.builder()
                .setHostname(host)
                .setPort(port);

        // 可选参数
        descriptorProperties.getOptionalString(CONNECTOR_DELIMITER).ifPresent(builder::setDelimiter);
        descriptorProperties.getOptionalLong(CONNECTOR_MAX_NUM_RETRIES).ifPresent(builder::setMaxNumRetries);
        descriptorProperties.getOptionalLong(CONNECTOR_DELAY_BETWEEN_RETRIES).ifPresent(builder::setDelayBetweenRetries);

        return builder.build();
    }
}
