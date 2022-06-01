package com.flink.connector.socket;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flink.connector.socket.SocketSourceValidator.*;
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
        context.put("update-mode", "append");
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
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // Socket参数
        SocketOption socketOption = getSocketOption(descriptorProperties);

        // 做什么用
        TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(tableSchema);

        // 创建 SocketTableSource
        SocketTableSource tableSource = SocketTableSource.builder()
                .setSchema(physicalSchema)
                .setSocketOption(socketOption)
                .build();
        return tableSource;
    }

    private SocketOption getSocketOption(DescriptorProperties descriptorProperties) {
        String host = descriptorProperties.getString(CONNECTOR_HOST);
        int port = descriptorProperties.getInt(CONNECTOR_PORT);

        SocketOption.Builder builder = SocketOption.builder()
                .setHostname(host)
                .setPort(port);

        descriptorProperties.getOptionalString(CONNECTOR_DELIMITER).ifPresent(builder::setDelimiter);
        descriptorProperties.getOptionalLong(CONNECTOR_MAX_NUM_RETRIES).ifPresent(builder::setMaxNumRetries);
        descriptorProperties.getOptionalLong(CONNECTOR_DELAY_BETWEEN_RETRIES).ifPresent(builder::setDelayBetweenRetries);

        return builder.build();
    }
}
