package com.flink.connector.socket;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * 功能：Socket Source Validator
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/30 上午8:46
 */
public class SocketSourceValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE = "socket";
    public static final String CONNECTOR_HOST = "host";
    public static final String CONNECTOR_PORT = "port";
    public static final String CONNECTOR_DELIMITER = "delimiter";
    public static final String CONNECTOR_MAX_NUM_RETRIES = "maxNumRetries";
    public static final String CONNECTOR_DELAY_BETWEEN_RETRIES = "delayBetweenRetries";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateString(CONNECTOR_HOST, false, 1);
        properties.validateInt(CONNECTOR_PORT, false);
        properties.validateString(CONNECTOR_DELIMITER, true, 1);

        Optional<Long> maxNumRetries = properties.getOptionalLong(CONNECTOR_MAX_NUM_RETRIES);
        if (maxNumRetries.isPresent()) {
            Preconditions.checkArgument(
                    maxNumRetries.get() >= -1,
                    "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)"
            );
        }

        Optional<Long> delayBetweenRetries = properties.getOptionalLong(CONNECTOR_DELAY_BETWEEN_RETRIES);
        if (delayBetweenRetries.isPresent()) {
            Preconditions.checkArgument(delayBetweenRetries.get() >= 0, "delayBetweenRetries must be zero or positive");
        }
    }
}
