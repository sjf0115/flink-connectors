package com.flink.connector.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * 功能：Socket TableSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:57
 */
public class SocketTableSource implements StreamTableSource<String>{

    private final String hostname;
    private final int port;
    private final String delimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;

    public SocketTableSource(long delayBetweenRetries, long maxNumRetries, String delimiter, int port, String hostname) {
        this.delayBetweenRetries = delayBetweenRetries;
        this.maxNumRetries = maxNumRetries;
        this.delimiter = delimiter;
        this.port = port;
        this.hostname = hostname;
    }

    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        SocketSourceFunction socketSourceFunction = new SocketSourceFunction(hostname, port, delimiter, maxNumRetries, delayBetweenRetries);
        return env.addSource(socketSourceFunction);
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
