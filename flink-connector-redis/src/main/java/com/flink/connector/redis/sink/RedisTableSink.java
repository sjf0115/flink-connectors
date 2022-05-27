package com.flink.connector.redis.sink;

import com.flink.connector.redis.options.RedisOption;
import com.flink.connector.redis.options.RedisSinkOption;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：Redis TableSink
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/1 下午6:54
 */
public class RedisTableSink implements UpsertStreamTableSink<Row>{

    private final TableSchema schema;
    private final RedisOption redisOption;
    private final RedisSinkOption redisSinkOption;

    private String[] keyFields;
    private boolean isAppendOnly;

    private RedisTableSink(TableSchema schema, RedisOption redisOption, RedisSinkOption redisSinkOption) {
        this.schema = schema;
        this.redisOption = redisOption;
        this.redisSinkOption = redisSinkOption;
    }

    @Override
    public void setKeyFields(String[] keyFields) {
        this.keyFields = keyFields;
    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {
        this.isAppendOnly = isAppendOnly;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return schema.toRowType();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new RedisSinkFunction(redisOption, redisSinkOption))
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new RedisTableSink(schema, redisOption, redisSinkOption);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TableSchema schema;
        private RedisOption redisOption;
        private RedisSinkOption redisSinkOption;

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setRedisOption(RedisOption redisOption) {
            this.redisOption = redisOption;
            return this;
        }

        public Builder setRedisSinkOption(RedisSinkOption redisSinkOption) {
            this.redisSinkOption = redisSinkOption;
            return this;
        }

        public RedisTableSink build() {
            checkNotNull(schema, "No schema supplied.");
            checkNotNull(redisOption, "No redisOption supplied.");
            checkNotNull(redisSinkOption, "No redisSinkOption supplied.");
            return new RedisTableSink(schema, redisOption, redisSinkOption);
        }
    }
}
