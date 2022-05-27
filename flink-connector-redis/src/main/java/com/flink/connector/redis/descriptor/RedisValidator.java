package com.flink.connector.redis.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * 功能：RedisValidator
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/2 下午5:40
 */
public class RedisValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE = "redis";
    public static final String CONNECTOR_HOST = "host";
    public static final String CONNECTOR_PORT = "port";
    public static final String CONNECTOR_CLUSTER_MODE = "clusterMode";
    public static final String CONNECTOR_PASSWORD = "password";
    public static final String CONNECTOR_DB_NUM = "dbNum";
    public static final String CONNECTOR_MODE = "mode";

    public static final String CONNECTOR_KEY_EXPIRE = "keyExpire";
    public static final String CONNECTOR_IGNORE_DELETE = "ignoreDelete";

    public static final String CONNECTOR_CACHE = "cache";
    public static final String CONNECTOR_CACHE_SIZE = "cacheSize";
    public static final String CONNECTOR_CACHE_TTL_MS = "cacheTTLMs";
    public static final String CONNECTOR_CACHE_EMPTY = "cacheEmpty";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        validateCommonProperties(properties);
        validateSinkProperties(properties);
        validateLookupProperties(properties);
    }

    private void validateCommonProperties(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_HOST, false, 1);
        properties.validateInt(CONNECTOR_PORT, true);
        properties.validateBoolean(CONNECTOR_CLUSTER_MODE, true);
        properties.validateString(CONNECTOR_PASSWORD, false, 1);
        properties.validateInt(CONNECTOR_DB_NUM, true);
        properties.validateString(CONNECTOR_MODE, false, 1);
        properties.validateInt(CONNECTOR_DB_NUM, true);
    }

    private void validateSinkProperties(DescriptorProperties properties) {
        properties.validateInt(CONNECTOR_KEY_EXPIRE, true);
        properties.validateBoolean(CONNECTOR_IGNORE_DELETE, true);

        Optional<Integer> keyExpire = properties.getOptionalInt(CONNECTOR_KEY_EXPIRE);
        if (keyExpire.isPresent()) {
            Preconditions.checkArgument(
                    keyExpire.get() > 0 || keyExpire.get() == -1,
                    "KeyExpire must be more than 0 or equal -1");
        }
    }

    private void validateLookupProperties(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_CACHE, true);
        properties.validateLong(CONNECTOR_CACHE_SIZE, true);
        properties.validateLong(CONNECTOR_CACHE_TTL_MS, true);
        properties.validateBoolean(CONNECTOR_CACHE_EMPTY, true);
    }
}
