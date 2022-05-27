package com.flink.connector.redis;

import com.flink.connector.redis.options.RedisLookupOption;
import com.flink.connector.redis.options.RedisOption;
import com.flink.connector.redis.options.RedisSinkOption;
import com.flink.connector.redis.sink.RedisTableSink;
import com.flink.connector.redis.source.RedisTableLookupSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.*;

import static com.flink.connector.redis.descriptor.RedisValidator.*;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.*;
/**
 * 功能：RedisTableFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/1 上午10:41
 */
public class RedisTableFactory implements StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
        // 参数的向后兼容
        require.put(CONNECTOR_PROPERTY_VERSION, "1");
        return require;
    }

    public List<String> supportedProperties() {
        // 配置 Connector 支持的参数
        List<String> properties = new ArrayList<>();
        properties.add(CONNECTOR_HOST);
        properties.add(CONNECTOR_PORT);
        properties.add(CONNECTOR_CLUSTER_MODE);
        properties.add(CONNECTOR_PASSWORD);
        properties.add(CONNECTOR_DB_NUM);
        properties.add(CONNECTOR_MODE);
        properties.add(CONNECTOR_KEY_EXPIRE);
        properties.add(CONNECTOR_IGNORE_DELETE);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // format wildcard
        properties.add(FORMAT + ".*");
        properties.add(CONNECTOR + ".*");
        return properties;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA)
        );

        RedisTableLookupSource tableSource = RedisTableLookupSource.builder()
                .setSchema(schema)
                .setRedisOption(getRedisOption(descriptorProperties))
                .setRedisLookupOption(getRedisLookupOption(descriptorProperties))
                .build();
        return tableSource;
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA)
        );

        Optional<UniqueConstraint> primaryKey = schema.getPrimaryKey();

        RedisTableSink tableSink = RedisTableSink.builder()
                .setSchema(schema)
                .setRedisOption(getRedisOption(descriptorProperties))
                .setRedisSinkOption(getRedisSinkOption(descriptorProperties))
                .build();
        return tableSink;
    }

    /**
     * 构造 RedisOption
     * @param descriptorProperties
     * @return
     */
    private RedisOption getRedisOption(DescriptorProperties descriptorProperties) {
        final String host = descriptorProperties.getString(CONNECTOR_HOST);
        final String password = descriptorProperties.getString(CONNECTOR_PASSWORD);
        final String mode = descriptorProperties.getString(CONNECTOR_MODE);

        final RedisOption.Builder builder = RedisOption.builder()
                .setHost(host)
                .setPassword(password)
                .setMode(mode);

        descriptorProperties.getOptionalInt(CONNECTOR_PORT).ifPresent(builder::setPort);
        descriptorProperties.getOptionalBoolean(CONNECTOR_CLUSTER_MODE).ifPresent(builder::setClusterMode);
        descriptorProperties.getOptionalInt(CONNECTOR_DB_NUM).ifPresent(builder::setDataBaseNum);

        return builder.build();
    }

    /**
     * 构造 RedisSinkOption
     * @param descriptorProperties
     * @return
     */
    private RedisSinkOption getRedisSinkOption(DescriptorProperties descriptorProperties) {
        final RedisSinkOption.Builder builder = RedisSinkOption.builder();

        descriptorProperties.getOptionalInt(CONNECTOR_KEY_EXPIRE).ifPresent(builder::setKeyExpire);
        descriptorProperties.getOptionalBoolean(CONNECTOR_IGNORE_DELETE).ifPresent(builder::setIgnoreDelete);

        return builder.build();
    }

    /**
     * 构造 RedisLookupOption
     * @param descriptorProperties
     * @return
     */
    private RedisLookupOption getRedisLookupOption(DescriptorProperties descriptorProperties) {
        final RedisLookupOption.Builder builder = RedisLookupOption.builder();

        descriptorProperties.getOptionalString(CONNECTOR_CACHE).ifPresent(builder::setCache);
        descriptorProperties.getOptionalLong(CONNECTOR_CACHE_SIZE).ifPresent(builder::setCacheSize);
        descriptorProperties.getOptionalLong(CONNECTOR_CACHE_TTL_MS).ifPresent(builder::setCacheTTLMs);
        descriptorProperties.getOptionalBoolean(CONNECTOR_CACHE_EMPTY).ifPresent(builder::setCacheEmpty);

        return builder.build();
    }
}
