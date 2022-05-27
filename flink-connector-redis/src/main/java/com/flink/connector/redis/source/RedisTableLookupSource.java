package com.flink.connector.redis.source;

import com.flink.connector.redis.options.RedisLookupOption;
import com.flink.connector.redis.options.RedisOption;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：RedisTableLookupSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/2 下午11:05
 */
public class RedisTableLookupSource implements
        StreamTableSource<Row>,
        LookupableTableSource<Row> {

    private final TableSchema schema;
    private final RedisOption redisOption;
    private final RedisLookupOption redisLookupOption;

    private RedisTableLookupSource(TableSchema schema, RedisOption redisOption, RedisLookupOption redisLookupOption) {
        this.schema = schema;
        this.redisOption = redisOption;
        this.redisLookupOption = redisLookupOption;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return schema.toRowType();
    }

    @Override
    public String explainSource() {
        return "Redis";
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        throw new UnsupportedOperationException("Redis do not support async lookup");
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public TableSchema getTableSchema() {
        return this.schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TableSchema schema;
        private RedisOption redisOption;
        private RedisLookupOption redisLookupOption;

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setRedisOption(RedisOption redisOption) {
            this.redisOption = redisOption;
            return this;
        }

        public Builder setRedisLookupOption(RedisLookupOption redisLookupOption) {
            this.redisLookupOption = redisLookupOption;
            return this;
        }

        public RedisTableLookupSource build() {
            checkNotNull(schema, "No schema supplied.");
            checkNotNull(redisOption, "No redisOption supplied.");
            checkNotNull(redisLookupOption, "No redisLookupOption supplied.");
            return new RedisTableLookupSource(schema, redisOption, redisLookupOption);
        }
    }
}
